---------------------------- MODULE EventOrder ----------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Txns, Receipts, NoTxn
ASSUME NoTxn \notin Txns

Max2(a, b) == IF a >= b THEN a ELSE b
MinSeq(items) == CHOOSE x \in {e.seq : e \in items} :
                    \A y \in {e.seq : e \in items} : x <= y
FloorAllows(floor, seq) == IF floor = NoTxn THEN TRUE ELSE floor >= seq

VARIABLES active, committed, ready, published, high, lastReceipt, visible, returned

vars == <<active, committed, ready, published, high, lastReceipt, visible, returned>>

Init ==
    /\ active = [t \in Txns |-> NoTxn]
    /\ committed = [t \in Txns |-> NoTxn]
    /\ ready = {}
    /\ published = <<>>
    /\ high = 0
    /\ lastReceipt = 0
    /\ visible = {}
    /\ returned = {}

Begin(t) ==
    /\ t \in Txns
    /\ active[t] = NoTxn
    /\ active' = [active EXCEPT ![t] = high]
    /\ visible' = visible \ {t}
    /\ returned' = returned \ {t}
    /\ committed' = [committed EXCEPT ![t] = NoTxn]
    /\ UNCHANGED <<ready, published, high, lastReceipt>>

NativeCommit(t, seq) ==
    /\ t \in Txns
    /\ active[t] # NoTxn
    /\ committed[t] = NoTxn
    /\ seq \in Receipts
    /\ seq > lastReceipt
    /\ committed' = [committed EXCEPT ![t] = seq]
    /\ UNCHANGED <<active, ready, published>>
    /\ lastReceipt' = seq
    /\ UNCHANGED <<high, visible, returned>>

EmptyCommitReusesReceipt(t) ==
    /\ t \in Txns
    /\ active[t] # NoTxn
    /\ committed[t] = NoTxn
    /\ committed' = [committed EXCEPT ![t] = lastReceipt]
    /\ UNCHANGED <<active, ready, published, high, lastReceipt, visible, returned>>

EventReady(t) ==
    /\ t \in Txns
    /\ active[t] # NoTxn
    /\ committed[t] # NoTxn
    /\ ready' = ready \cup { [seq |-> committed[t], txn |-> t] }
    /\ active' = [active EXCEPT ![t] = NoTxn]
    /\ high' = Max2(high, committed[t])
    /\ UNCHANGED <<committed, published, lastReceipt, visible, returned>>

Fail(t) ==
    /\ t \in Txns
    /\ active[t] # NoTxn
    /\ committed[t] = NoTxn
    /\ active' = [active EXCEPT ![t] = NoTxn]
    /\ committed' = [committed EXCEPT ![t] = NoTxn]
    /\ UNCHANGED <<ready, published, high, lastReceipt, visible, returned>>

Dispatch ==
    /\ ready # {}
    /\ LET item == CHOOSE e \in ready :
              e.seq = MinSeq(ready)
       IN /\ \A t \in Txns : FloorAllows(active[t], item.seq)
          /\ ready' = ready \ {item}
          /\ published' = Append(published, item.seq)
          /\ visible' = visible \cup {item.txn}
    /\ UNCHANGED <<active, committed, high, lastReceipt, returned>>

RedDispatch ==
    /\ ready # {}
    /\ LET item == CHOOSE e \in ready :
              e.seq = MinSeq(ready)
       IN /\ ready' = ready \ {item}
          /\ published' = Append(published, item.seq)
          /\ visible' = visible \cup {item.txn}
    /\ UNCHANGED <<active, committed, high, lastReceipt, returned>>

Return(t) ==
    /\ t \in visible
    /\ t \notin returned
    /\ returned' = returned \cup {t}
    /\ UNCHANGED <<active, committed, ready, published, high, lastReceipt, visible>>

RedReturn(t) ==
    /\ committed[t] # NoTxn
    /\ t \notin returned
    /\ returned' = returned \cup {t}
    /\ UNCHANGED <<active, committed, ready, published, high, lastReceipt, visible>>

Next ==
    \/ \E t \in Txns : Begin(t)
    \/ \E t \in Txns, seq \in Receipts : NativeCommit(t, seq)
    \/ \E t \in Txns : EventReady(t)
    \/ \E t \in Txns : Fail(t)
    \/ Dispatch
    \/ \E t \in Txns : Return(t)

RedNext ==
    \/ \E t \in Txns : Begin(t)
    \/ \E t \in Txns, seq \in Receipts : NativeCommit(t, seq)
    \/ \E t \in Txns : EventReady(t)
    \/ \E t \in Txns : EmptyCommitReusesReceipt(t)
    \/ \E t \in Txns : Fail(t)
    \/ RedDispatch

EarlyReturnNext ==
    \/ Next
    \/ \E t \in Txns : RedReturn(t)

TypeOK ==
    /\ active \in [Txns -> Nat \cup {NoTxn}]
    /\ committed \in [Txns -> Nat \cup {NoTxn}]
    /\ ready \subseteq [seq : Nat, txn : Txns]
    /\ published \in Seq(Nat)
    /\ high \in Nat
    /\ lastReceipt \in Nat
    /\ visible \subseteq Txns
    /\ returned \subseteq Txns

Ordered == \A i, j \in 1..Len(published) : i < j => published[i] < published[j]

CompletionVisible == returned \subseteq visible

EarlyReturnSpec == Init /\ [][EarlyReturnNext]_vars
Spec == Init /\ [][Next]_vars
RedSpec == Init /\ [][RedNext]_vars

=============================================================================
