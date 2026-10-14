---------------------------- MODULE EventOrder ----------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Txns, Receipts, NoTxn
ASSUME NoTxn \notin Txns

VARIABLES active, committed, ready, published, high, lastReceipt

vars == <<active, committed, ready, published, high, lastReceipt>>

Init ==
    /\ active = [t \in Txns |-> NoTxn]
    /\ committed = [t \in Txns |-> NoTxn]
    /\ ready = {}
    /\ published = <<>>
    /\ high = 0
    /\ lastReceipt = 0

Begin(t) ==
    /\ t \in Txns
    /\ active[t] = NoTxn
    /\ active' = [active EXCEPT ![t] = high]
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
    /\ UNCHANGED high

EventReady(t) ==
    /\ t \in Txns
    /\ active[t] # NoTxn
    /\ committed[t] # NoTxn
    /\ ready' = ready \cup { [seq |-> committed[t], txn |-> t] }
    /\ active' = [active EXCEPT ![t] = NoTxn]
    /\ high' = Max({high, committed[t]})
    /\ UNCHANGED <<committed, published, lastReceipt>>

Fail(t) ==
    /\ t \in Txns
    /\ active[t] # NoTxn
    /\ committed[t] = NoTxn
    /\ active' = [active EXCEPT ![t] = NoTxn]
    /\ committed' = [committed EXCEPT ![t] = NoTxn]
    /\ UNCHANGED <<ready, published, high, lastReceipt>>

Dispatch ==
    /\ ready # {}
    /\ LET item == CHOOSE e \in ready :
              e.seq = Min({x.seq : x \in ready})
       IN /\ \A t \in Txns : active[t] = NoTxn \/ active[t] >= item.seq
          /\ ready' = ready \ {item}
          /\ published' = Append(published, item.seq)
    /\ UNCHANGED <<active, committed, high, lastReceipt>>

RedDispatch ==
    /\ ready # {}
    /\ LET item == CHOOSE e \in ready :
              e.seq = Min({x.seq : x \in ready})
       IN /\ ready' = ready \ {item}
          /\ published' = Append(published, item.seq)
    /\ UNCHANGED <<active, committed, high, lastReceipt>>

Next ==
    \/ \E t \in Txns : Begin(t)
    \/ \E t \in Txns, seq \in Receipts : NativeCommit(t, seq)
    \/ \E t \in Txns : EventReady(t)
    \/ \E t \in Txns : Fail(t)
    \/ Dispatch

RedNext ==
    \/ \E t \in Txns : Begin(t)
    \/ \E t \in Txns, seq \in Receipts : NativeCommit(t, seq)
    \/ \E t \in Txns : EventReady(t)
    \/ \E t \in Txns : Fail(t)
    \/ RedDispatch

TypeOK ==
    /\ active \in [Txns -> Nat \cup {NoTxn}]
    /\ committed \in [Txns -> Nat \cup {NoTxn}]
    /\ ready \subseteq [seq : Nat, txn : Txns]
    /\ published \in Seq(Nat)
    /\ high \in Nat
    /\ lastReceipt \in Nat

Ordered == \A i, j \in 1..Len(published) : i < j => published[i] < published[j]

Spec == Init /\ [][Next]_vars
RedSpec == Init /\ [][RedNext]_vars

=============================================================================
