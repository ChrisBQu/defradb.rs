---- MODULE RetryPreparedInputs ----
\* Retry contract for autocommit writes: a transaction that discovers a
\* missing provider answer aborts; resolution happens outside the attempt and
\* its exact-context answer is reused after commit conflict. This models the
\* control protocol, not cryptographic/KMS correctness.
EXTENDS Naturals
CONSTANT KeepMemoOnConflict, ResolveOutside
ASSUME KeepMemoOnConflict \in BOOLEAN
ASSUME ResolveOutside \in BOOLEAN
VARIABLES active, context, memoContext, providerCalls, networkInAttempt, commits, attempts
vars == <<active, context, memoContext, providerCalls, networkInAttempt, commits, attempts>>
Init == /\ active = FALSE /\ context = "c0" /\ memoContext = "none"
        /\ providerCalls = 0 /\ networkInAttempt = 0 /\ commits = 0 /\ attempts = 0
\* A new request starts outside an attempt without an answer in its memo.
Begin == /\ ~active /\ attempts < 3 /\ active' = TRUE /\ attempts' = attempts + 1
         /\ UNCHANGED <<context, memoContext, providerCalls, networkInAttempt, commits>>
\* A missing exact-context answer ends this attempt before external resolution.
NeedInput == /\ active /\ memoContext # context /\ active' = FALSE
             /\ UNCHANGED <<context, memoContext, providerCalls, networkInAttempt, commits, attempts>>
\* Resolve models a provider call; the GREEN policy permits it only outside an attempt.
Resolve == /\ memoContext # context
           /\ providerCalls < 3 /\ (ResolveOutside => ~active)
           /\ memoContext' = context /\ providerCalls' = providerCalls + 1
           /\ networkInAttempt' = networkInAttempt + IF active THEN 1 ELSE 0
           /\ UNCHANGED <<active, context, commits, attempts>>
\* A write commits only when its prepared answer matches the current dependency context.
Commit == /\ active /\ memoContext = context /\ commits < 2 /\ active' = FALSE /\ commits' = commits + 1
          /\ UNCHANGED <<context, memoContext, providerCalls, networkInAttempt, attempts>>
\* A storage conflict releases the attempt; only the GREEN policy retains the answer.
Conflict == /\ active /\ active' = FALSE
            /\ memoContext' = IF KeepMemoOnConflict THEN memoContext ELSE "none"
            /\ UNCHANGED <<context, providerCalls, networkInAttempt, commits, attempts>>
\* A schema/head/context change invalidates a prior memo by changing its exact key.
ChangeContext == /\ ~active /\ context = "c0" /\ context' = "c1"
                /\ UNCHANGED <<active, memoContext, providerCalls, networkInAttempt, commits, attempts>>
Next == Begin \/ NeedInput \/ Resolve \/ Commit \/ Conflict \/ ChangeContext
Spec == Init /\ [][Next]_vars
TypeOK == /\ active \in BOOLEAN /\ context \in {"c0", "c1"}
          /\ memoContext \in {"none", "c0", "c1"} /\ providerCalls \in Nat
          /\ networkInAttempt \in Nat
          /\ commits \in Nat /\ attempts \in Nat
\* Each context can trigger at most one resolution in this bounded request.
INV_ProviderOncePerContext == providerCalls <= IF context = "c0" THEN 1 ELSE 2
\* No external provider call is allowed while the transaction attempt is active.
INV_NoProviderInsideAttempt == networkInAttempt = 0
\* The only way to commit is through an attempt with the matching exact input.
INV_CommitHasExactPreparedInput == commits <= attempts
====
