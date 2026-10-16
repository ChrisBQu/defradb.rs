---- MODULE CommitFence ----
\* The story, in plain words.
\*
\* An explicit transaction lives on one worker, its owner. To commit it, the
\* caller hands the commit to the owner and waits for the answer. The caller
\* can stop waiting at any moment (its future is dropped: the client went
\* away, a timeout fired). Stopping does not undo a commit it handed over:
\* the owner runs it all the same, and it lands a little later. The danger:
\* a read or a new transaction that begins after the caller stopped could
\* take its snapshot before the commit lands, and miss it.
\*
\* The fix is a fence. The database keeps one count, `outstanding`: how many
\* commits are still landing after their caller stopped waiting. Each commit
\* handed to an owner carries a small state word that starts "pending". The
\* caller and the owner race to move it out of "pending" with one
\* compare-and-swap each, so exactly one of them wins:
\*   * the caller, when it stops waiting, first raises the count, then tries
\*     pending -> abandoned; if the owner had finished already (the swap
\*     fails) the caller lowers the count back;
\*   * the owner, once it is finished with the commit (it landed, it failed,
\*     or its request was dropped unrun), tries pending -> done; if the caller
\*     had left (the swap fails) the owner lowers the count.
\* Whoever lowers the count to zero wakes everyone waiting. A reader (a read
\* or a new transaction) looks at the count first: zero means go at once;
\* otherwise it waits for a wake and looks again. A commit whose caller never
\* stops waiting never touches the count: the fast path costs one load.
\*
\* What must never happen:
\*   1. a reader that began after a caller stopped waiting takes a snapshot
\*      that misses that commit, when the commit does land
\*      - INV_ReaderSeesCancelledCommit;
\*   2. the count goes below zero - INV_NeverNegative;
\*   3. a commit whose caller never stopped waiting touches the count
\*      - INV_UncancelledUntouched;
\*   4. once both sides are done with a commit, it still holds part of the
\*      count - INV_SettledExact;
\*   5. the count stays above zero for good (a leaked fence)
\*      - LIVE_NoFenceLeak;
\*   6. a reader waits forever - LIVE_ReaderProceeds;
\*   7. a caller that stops waiting undoes the commit it handed over (the
\*      owner skips it) - INV_CancelNeverUndoes.
\*
\* Every way a commit can end is a path here: it lands, it fails, the owner
\* shuts down (its request dropped unrun, or the commit dropped before it
\* landed), and the caller stops waiting before it handed the commit over,
\* after it handed it over, or after the answer was already on its way.
\*
\* TLC runs whole steps one at a time, as if memory were sequentially
\* consistent. crates/db/src/txn/fence.rs names the Rust memory orderings
\* (Release on each swap and on each lowering, Acquire on each failed swap and
\* on the reader's load) that give every step here the order it has in the
\* model.
\*
\* Correspondence with the Rust code (crates/db/src/txn/ unless named):
\*
\*   | Model             | Rust                                                        |
\*   |-------------------|-------------------------------------------------------------|
\*   | Send              | owner/mod.rs Owners::finish: table.take, Fence::new,        |
\*   |                   | mailbox.try_send(Message::Finish)                           |
\*   | DropEarly         | the finish future dropped before it ran: nothing handed over|
\*   | Drop, CallerSwap, | owner/mod.rs FenceOnDrop::drop -> fence.rs Fence::abandon:  |
\*   | Undo, UndoWake    | CommitFences::raise, compare_exchange(PENDING, ABANDONED),  |
\*   |                   | on failure CommitFences::lower                              |
\*   | Receive           | Owners::finish: answer.recv_async() returns, then disarm    |
\*   | OwnerStart        | owner/actor.rs own, the Message::Finish arm: idle.pop()     |
\*   | OwnerLand/Fail    | last(txn).await: OwnedTxn::commit or rollback, Ok or Err    |
\*   | OwnerReply        | reply.try_send inside the registry's `last`                 |
\*   | OwnerShutdown     | the Finish dropped unrun: the actor's drain when it ends    |
\*   |                   | (owner/actor.rs Drain), its wait for the taker's end when   |
\*   |                   | it did not take its own entry, or the commit future dropped |
\*   | OwnerSwap, OwnerDec, | fence.rs Landing::drop -> Fence::finish:                 |
\*   | OwnerWake         | compare_exchange(PENDING, DONE), on failure                 |
\*   |                   | CommitFences::lower (fetch_sub; notify_waiters at zero)     |
\*   | ReaderFast        | database/mod.rs DB::new_txn: CommitFences::any, one load    |
\*   | ReaderArm, Check, | fence.rs CommitFences::cleared: Notify::notified (reads the |
\*   | Wait, Wake        | generation), the load, notified.await                       |
\*   | gen               | regolith::sync::Notify's generation (notify_waiters)        |
\*   | RaiseFirst        | raise before the caller's swap (RED: after it)              |
\*   | OwnerSwaps        | the owner's compare_exchange (RED: lowers without it)       |
\*   | ReaderWaits       | the wait in DB::new_txn (RED: none)                         |
\*   | ResolveFailed     | Landing resolves on every end (RED: not on a failure)       |
\*   | WakeOnUndo        | lower() wakes at zero on the caller's undo too (RED: not)   |
\*   | RunCancelled      | the Finish arm runs whatever the state word says (RED:      |
\*   |                   | skips the commit when its caller left first)                |

EXTENDS Integers, FiniteSets
\* Integers gives numbers that can go below zero (so a RED run can show it);
\* FiniteSets gives Cardinality, the size of a set.

CONSTANTS NC, RaiseFirst, OwnerSwaps, ReaderWaits, ResolveFailed, WakeOnUndo,
          RunCancelled
\* NC is how many commits the model runs. The six switches are all TRUE in
\* the real design; each RED configuration turns one off.

ASSUME /\ NC \in 1..3
       /\ RaiseFirst \in BOOLEAN
       /\ OwnerSwaps \in BOOLEAN
       /\ ReaderWaits \in BOOLEAN
       /\ ResolveFailed \in BOOLEAN
       /\ WakeOnUndo \in BOOLEAN
       /\ RunCancelled \in BOOLEAN
\* A small number of commits, and each switch either on or off.

Commits == 1..NC
\* The commits, named 1, 2, ...

CallerStates == {"idle", "waiting", "raised", "late", "undo", "unotify", "gone", "replied"}
\* Where a caller can be:
\*   idle     - it has not handed its commit over yet;
\*   waiting  - it handed the commit over and waits for the answer;
\*   raised   - it stopped waiting and raised the count; its swap comes next;
\*   late     - (RED only) it swapped first and raises the count next;
\*   undo     - its swap failed (the owner was done first); it lowers next;
\*   unotify  - its lowering reached zero; it wakes the waiters next;
\*   gone     - it stopped waiting, and everything it had to do is done;
\*   replied  - it got its answer and never stopped waiting.

OwnerStates == {"none", "queued", "running", "skipped", "landed", "failed", "swap", "dec", "notify", "resolved"}
\* Where the owner's side of one commit can be:
\*   none     - nothing was handed to it;
\*   queued   - the commit waits in the owner's mailbox;
\*   running  - the owner is committing it;
\*   skipped  - (RED only) the owner dropped it unrun because its caller left;
\*   landed   - the commit is in: a snapshot taken from now on sees it;
\*   failed   - the commit failed: it will never land;
\*   swap     - the owner is finished with it and tries pending -> done next;
\*   dec      - its swap failed (the caller left); it lowers the count next;
\*   notify   - its lowering reached zero; it wakes the waiters next;
\*   resolved - the owner is done with this commit for good.

ReaderStates == {"before", "fast", "arm", "check", "wait", "done"}
\* Where the one reader can be:
\*   before - it has not begun;
\*   fast   - it has begun and makes its one look at the count;
\*   arm    - the count was not zero: it notes the wake generation next;
\*   check  - it looks at the count again, after noting the generation;
\*   wait   - it sleeps until the generation moves;
\*   done   - it took its snapshot.

VARIABLES callerPc, ownerPc, state, sent, answered, landed, net,
          outstanding, gen, rpc, rgen, seen, snap
\* callerPc    - where each caller is (CallerStates);
\* ownerPc     - where the owner is with each commit (OwnerStates);
\* state       - each commit's state word: "pending", "abandoned" or "done";
\* sent        - whether each commit was handed to its owner;
\* answered    - whether each caller's answer is ready (a reply, or the
\*               request dropped, which the caller hears as "not found");
\* landed      - whether each commit landed;
\* net         - how much each commit adds to the count right now;
\* outstanding - the count every reader looks at;
\* gen         - the wake generation, moved by each wake;
\* rpc         - where the reader is (ReaderStates);
\* rgen        - the generation the reader noted before its last look;
\* seen        - the commits whose callers had fully stopped waiting when
\*               the reader began: the reader must see each one that lands;
\* snap        - the commits the reader's snapshot holds.

vars == <<callerPc, ownerPc, state, sent, answered, landed, net,
          outstanding, gen, rpc, rgen, seen, snap>>
\* Every variable, for "nothing else changes" and for fairness.

Init == /\ callerPc = [c \in Commits |-> "idle"]
        /\ ownerPc = [c \in Commits |-> "none"]
        /\ state = [c \in Commits |-> "pending"]
        /\ sent = [c \in Commits |-> FALSE]
        /\ answered = [c \in Commits |-> FALSE]
        /\ landed = [c \in Commits |-> FALSE]
        /\ net = [c \in Commits |-> 0]
        /\ outstanding = 0
        /\ gen = 0
        /\ rpc = "before"
        /\ rgen = 0
        /\ seen = {}
        /\ snap = {}
\* At the start nothing was handed over, every state word is pending, the
\* count is zero, and the reader has not begun.

Landed == {c \in Commits : landed[c]}
\* The commits that are in: what a snapshot taken now holds.

\* ---------------------------------------------------------------- caller

Send(c) == /\ callerPc[c] = "idle"
           /\ callerPc' = [callerPc EXCEPT ![c] = "waiting"]
           /\ ownerPc' = [ownerPc EXCEPT ![c] = "queued"]
           /\ sent' = [sent EXCEPT ![c] = TRUE]
           /\ UNCHANGED <<state, answered, landed, net, outstanding, gen, rpc, rgen, seen, snap>>
\* The caller hands its commit to the owner: it now waits, and the commit
\* sits in the owner's mailbox.

DropEarly(c) == /\ callerPc[c] = "idle"
                /\ callerPc' = [callerPc EXCEPT ![c] = "gone"]
                /\ UNCHANGED <<ownerPc, state, sent, answered, landed, net, outstanding, gen, rpc, rgen, seen, snap>>
\* The caller stops before it handed anything over: no commit exists, so
\* there is nothing to fence.

Drop(c) == /\ callerPc[c] = "waiting"
           /\ IF RaiseFirst
                 THEN \* The real order: raise the count, then swap.
                      /\ outstanding' = outstanding + 1
                      /\ net' = [net EXCEPT ![c] = @ + 1]
                      /\ callerPc' = [callerPc EXCEPT ![c] = "raised"]
                      /\ UNCHANGED state
                 ELSE \* RED: swap first, raise afterwards.
                      /\ IF state[c] = "pending"
                            THEN /\ state' = [state EXCEPT ![c] = "abandoned"]
                                 /\ callerPc' = [callerPc EXCEPT ![c] = "late"]
                            ELSE /\ callerPc' = [callerPc EXCEPT ![c] = "gone"]
                                 /\ UNCHANGED state
                      /\ UNCHANGED <<net, outstanding>>
           /\ UNCHANGED <<ownerPc, sent, answered, landed, gen, rpc, rgen, seen, snap>>
\* The caller stops waiting after it handed the commit over (the answer may
\* even be on its way already). In the real design it raises the count
\* first. In the RED order it swaps first; if the owner was done, it simply
\* leaves.

CallerSwap(c) == /\ callerPc[c] = "raised"
                 /\ IF state[c] = "pending"
                       THEN /\ state' = [state EXCEPT ![c] = "abandoned"]
                            /\ callerPc' = [callerPc EXCEPT ![c] = "gone"]
                       ELSE /\ callerPc' = [callerPc EXCEPT ![c] = "undo"]
                            /\ UNCHANGED state
                 /\ UNCHANGED <<ownerPc, sent, answered, landed, net, outstanding, gen, rpc, rgen, seen, snap>>
\* The caller's one swap, pending -> abandoned. If it wins, the owner now
\* owes the lowering and the caller is gone. If the owner was done first,
\* the caller must take its own raise back.

LateRaise(c) == /\ callerPc[c] = "late"
                /\ outstanding' = outstanding + 1
                /\ net' = [net EXCEPT ![c] = @ + 1]
                /\ callerPc' = [callerPc EXCEPT ![c] = "gone"]
                /\ UNCHANGED <<ownerPc, state, sent, answered, landed, gen, rpc, rgen, seen, snap>>
\* RED only: the raise that comes after the swap, possibly after the owner
\* already lowered the count for this very commit.

Undo(c) == /\ callerPc[c] = "undo"
           /\ outstanding' = outstanding - 1
           /\ net' = [net EXCEPT ![c] = @ - 1]
           /\ callerPc' = [callerPc EXCEPT ![c] =
                              IF outstanding = 1 /\ WakeOnUndo THEN "unotify" ELSE "gone"]
           /\ UNCHANGED <<ownerPc, state, sent, answered, landed, gen, rpc, rgen, seen, snap>>
\* The caller takes its raise back. If that brings the count to zero, it
\* wakes the waiters next (the RED switch forgets that wake).

UndoWake(c) == /\ callerPc[c] = "unotify"
               /\ gen' = gen + 1
               /\ callerPc' = [callerPc EXCEPT ![c] = "gone"]
               /\ UNCHANGED <<ownerPc, state, sent, answered, landed, net, outstanding, rpc, rgen, seen, snap>>
\* The caller's wake: the generation moves, so every sleeping reader looks
\* again.

Receive(c) == /\ callerPc[c] = "waiting"
              /\ answered[c]
              /\ callerPc' = [callerPc EXCEPT ![c] = "replied"]
              /\ UNCHANGED <<ownerPc, state, sent, answered, landed, net, outstanding, gen, rpc, rgen, seen, snap>>
\* The caller gets its answer and stops guarding: from now on it never
\* touches the count.

\* ----------------------------------------------------------------- owner

OwnerStart(c) == /\ ownerPc[c] = "queued"
                 /\ IF ~RunCancelled /\ state[c] = "abandoned"
                       THEN /\ ownerPc' = [ownerPc EXCEPT ![c] = "skipped"]
                            /\ answered' = [answered EXCEPT ![c] = TRUE]
                       ELSE /\ ownerPc' = [ownerPc EXCEPT ![c] = "running"]
                            /\ UNCHANGED answered
                 /\ UNCHANGED <<callerPc, state, sent, landed, net, outstanding, gen, rpc, rgen, seen, snap>>
\* The owner takes the commit from its mailbox and starts committing it,
\* whether or not its caller is still waiting. (The RED switch skips the
\* commit when the caller left first: nothing lands.)

OwnerLand(c) == /\ ownerPc[c] = "running"
                /\ landed' = [landed EXCEPT ![c] = TRUE]
                /\ ownerPc' = [ownerPc EXCEPT ![c] = "landed"]
                /\ UNCHANGED <<callerPc, state, sent, answered, net, outstanding, gen, rpc, rgen, seen, snap>>
\* The commit lands: every snapshot taken from now on holds it.

OwnerFail(c) == /\ ownerPc[c] = "running"
                /\ ownerPc' = [ownerPc EXCEPT ![c] = "failed"]
                /\ UNCHANGED <<callerPc, state, sent, answered, landed, net, outstanding, gen, rpc, rgen, seen, snap>>
\* The commit fails (a conflict, a storage error): it never lands.

OwnerReply(c) == /\ ownerPc[c] \in {"landed", "failed"}
                 /\ answered' = [answered EXCEPT ![c] = TRUE]
                 /\ ownerPc' = [ownerPc EXCEPT ![c] =
                                   IF ownerPc[c] = "failed" /\ ~ResolveFailed
                                      THEN "resolved" ELSE "swap"]
                 /\ UNCHANGED <<callerPc, state, sent, landed, net, outstanding, gen, rpc, rgen, seen, snap>>
\* The owner sends the answer, then is finished with the commit and goes to
\* its swap. (The RED switch skips the swap for a failed commit.)

OwnerShutdown(c) == /\ ownerPc[c] \in {"queued", "running"}
                    /\ answered' = [answered EXCEPT ![c] = TRUE]
                    /\ ownerPc' = [ownerPc EXCEPT ![c] = "swap"]
                    /\ UNCHANGED <<callerPc, state, sent, landed, net, outstanding, gen, rpc, rgen, seen, snap>>
\* The owner goes away before the commit landed: the request is dropped
\* unrun, or the commit is dropped mid-way. Nothing lands, the caller hears
\* "not found", and the dropped request still does its swap.

OwnerSwap(c) == /\ ownerPc[c] \in {"swap", "skipped"}
                /\ IF OwnerSwaps
                      THEN IF state[c] = "pending"
                              THEN /\ state' = [state EXCEPT ![c] = "done"]
                                   /\ ownerPc' = [ownerPc EXCEPT ![c] = "resolved"]
                              ELSE /\ ownerPc' = [ownerPc EXCEPT ![c] = "dec"]
                                   /\ UNCHANGED state
                      ELSE \* RED: no swap; mark done and always lower.
                           /\ state' = [state EXCEPT ![c] = "done"]
                           /\ ownerPc' = [ownerPc EXCEPT ![c] = "dec"]
                /\ UNCHANGED <<callerPc, sent, answered, landed, net, outstanding, gen, rpc, rgen, seen, snap>>
\* The owner's one swap, pending -> done. If it wins, the caller never
\* raised for good and the owner is done. If the caller had left, the owner
\* owes the lowering. The RED switch lowers every time, swap or not.

OwnerDec(c) == /\ ownerPc[c] = "dec"
               /\ outstanding' = outstanding - 1
               /\ net' = [net EXCEPT ![c] = @ - 1]
               /\ ownerPc' = [ownerPc EXCEPT ![c] = IF outstanding = 1 THEN "notify" ELSE "resolved"]
               /\ UNCHANGED <<callerPc, state, sent, answered, landed, gen, rpc, rgen, seen, snap>>
\* The owner lowers the count; if that brings it to zero, it wakes the
\* waiters next.

OwnerWake(c) == /\ ownerPc[c] = "notify"
                /\ gen' = gen + 1
                /\ ownerPc' = [ownerPc EXCEPT ![c] = "resolved"]
                /\ UNCHANGED <<callerPc, state, sent, answered, landed, net, outstanding, rpc, rgen, seen, snap>>
\* The owner's wake: the generation moves, so every sleeping reader looks
\* again.

\* ---------------------------------------------------------------- reader

ReaderBegin == /\ rpc = "before"
               /\ seen' = {c \in Commits : sent[c] /\ callerPc[c] = "gone"}
               /\ IF ReaderWaits
                     THEN /\ rpc' = "fast"
                          /\ UNCHANGED snap
                     ELSE /\ rpc' = "done"
                          /\ snap' = Landed
               /\ UNCHANGED <<callerPc, ownerPc, state, sent, answered, landed, net, outstanding, gen, rgen>>
\* The reader begins, at any moment. It notes which callers had fully
\* stopped waiting by then: their commits are the ones it must see. The RED
\* switch takes the snapshot at once, with no look at the count.

ReaderFast == /\ rpc = "fast"
              /\ IF outstanding = 0
                    THEN /\ rpc' = "done"
                         /\ snap' = Landed
                    ELSE /\ rpc' = "arm"
                         /\ UNCHANGED snap
              /\ UNCHANGED <<callerPc, ownerPc, state, sent, answered, landed, net, outstanding, gen, rgen, seen>>
\* The one look of the fast path: zero means take the snapshot now.

ReaderArm == /\ rpc = "arm"
             /\ rgen' = gen
             /\ rpc' = "check"
             /\ UNCHANGED <<callerPc, ownerPc, state, sent, answered, landed, net, outstanding, gen, seen, snap>>
\* Before looking again, the reader notes the wake generation, so a wake
\* that comes after this moment is never lost.

ReaderCheck == /\ rpc = "check"
               /\ IF outstanding = 0
                     THEN /\ rpc' = "done"
                          /\ snap' = Landed
                     ELSE /\ rpc' = "wait"
                          /\ UNCHANGED snap
               /\ UNCHANGED <<callerPc, ownerPc, state, sent, answered, landed, net, outstanding, gen, rgen, seen>>
\* The second look: zero means go; otherwise sleep.

ReaderWake == /\ rpc = "wait"
              /\ gen # rgen
              /\ rpc' = "arm"
              /\ UNCHANGED <<callerPc, ownerPc, state, sent, answered, landed, net, outstanding, gen, rgen, seen, snap>>
\* A sleeping reader wakes only when the generation moved past the one it
\* noted, then notes the new one and looks again.

\* ------------------------------------------------------------------ spec

CallerStep(c) == CallerSwap(c) \/ LateRaise(c) \/ Undo(c) \/ UndoWake(c)
\* What a caller that stopped waiting still has to do: these run to the end.

OwnerStep(c) == OwnerStart(c) \/ OwnerLand(c) \/ OwnerFail(c) \/ OwnerReply(c)
                \/ OwnerShutdown(c) \/ OwnerSwap(c) \/ OwnerDec(c) \/ OwnerWake(c)
\* Everything the owner does with one commit, from the mailbox to the end.

ReaderStep == ReaderFast \/ ReaderArm \/ ReaderCheck \/ ReaderWake
\* Everything a begun reader does until its snapshot.

Next == \/ \E c \in Commits : Send(c) \/ DropEarly(c) \/ Drop(c) \/ Receive(c)
        \/ \E c \in Commits : CallerStep(c) \/ OwnerStep(c)
        \/ ReaderBegin \/ ReaderStep
\* Any actor takes any step it can, in any order.

Fairness == /\ \A c \in Commits : WF_vars(CallerStep(c)) /\ WF_vars(OwnerStep(c))
            /\ WF_vars(ReaderStep)
\* A caller that stopped waiting finishes what it started, an owner keeps
\* working through the commits it was handed, and a begun reader keeps going
\* while it can. Handing over, stopping, receiving and beginning are choices:
\* nothing forces them.

Spec == Init /\ [][Next]_vars /\ Fairness
\* The model: start at Init, take steps of Next, with the fairness above.

\* ------------------------------------------------------------ properties

TypeOK == /\ callerPc \in [Commits -> CallerStates]
          /\ ownerPc \in [Commits -> OwnerStates]
          /\ state \in [Commits -> {"pending", "abandoned", "done"}]
          /\ sent \in [Commits -> BOOLEAN]
          /\ answered \in [Commits -> BOOLEAN]
          /\ landed \in [Commits -> BOOLEAN]
          /\ net \in [Commits -> -NC..NC]
          /\ outstanding \in -NC..NC
          /\ gen \in 0..(2 * NC)
          /\ rpc \in ReaderStates
          /\ rgen \in 0..(2 * NC)
          /\ seen \subseteq Commits
          /\ snap \subseteq Commits
\* Every variable holds a value of the expected kind (a negative count is
\* allowed here so the RED runs can show it under its own name).

INV_ReaderSeesCancelledCommit == rpc = "done" => \A c \in seen : landed[c] => c \in snap
\* Promise: a reader that began after a caller stopped waiting sees that
\* caller's commit whenever the commit lands.

INV_NeverNegative == outstanding >= 0
\* Promise: the count never goes below zero.

Cancelled(c) == sent[c] /\ callerPc[c] \in {"raised", "late", "undo", "unotify", "gone"}
\* A commit is cancelled when it was handed over and its caller then stopped
\* waiting.

INV_UncancelledUntouched == \A c \in Commits : ~Cancelled(c) => net[c] = 0
\* Promise: a commit whose caller never stopped waiting adds nothing to the
\* count, ever (the fast path stays one load).

Settled(c) == callerPc[c] \in {"gone", "replied"} /\ ownerPc[c] \in {"none", "resolved"}
\* Both sides are done with a commit.

INV_SettledExact == \A c \in Commits : Settled(c) => net[c] = 0
\* Promise: once both sides are done with a commit, it holds no part of the
\* count: every raise was matched by exactly one lowering.

INV_CancelNeverUndoes == \A c \in Commits : ownerPc[c] # "skipped"
\* Promise: the owner runs every commit handed to it; a caller that stops
\* waiting never decides whether it lands. Only the commit itself (it lands
\* or fails) or the owner going away does.

LIVE_NoFenceLeak == <>[](outstanding = 0)
\* Promise: the count comes back to zero and stays there once every commit
\* is done.

LIVE_ReaderProceeds == (rpc \in {"fast", "arm", "check", "wait"}) ~> (rpc = "done")
\* Promise: a reader that began always gets to take its snapshot.

====
