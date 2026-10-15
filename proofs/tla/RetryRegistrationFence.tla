---- MODULE RetryRegistrationFence ----
\* Allocation and registration are separate: tokens can commit out of order.
EXTENDS Naturals, FiniteSets
CONSTANT CheckEpoch, CheckOrder
VARIABLES present, epoch, allocated, staged, captured, marker, unsafeCommit
vars == <<present, epoch, allocated, staged, captured, marker, unsafeCommit>>
Init == /\ present = TRUE /\ epoch = 1 /\ allocated = 0
        /\ staged = {} /\ captured = [g \in 1..2 |-> 0]
        /\ marker = 0 /\ unsafeCommit = FALSE
Allocate == /\ present /\ allocated < 2
            /\ allocated' = allocated + 1
            /\ staged' = staged \cup {allocated + 1}
            /\ captured' = [captured EXCEPT ![allocated + 1] = epoch]
            /\ UNCHANGED <<present, epoch, marker, unsafeCommit>>
Forget == /\ present /\ present' = FALSE /\ marker' = 0
          /\ UNCHANGED <<epoch, allocated, staged, captured, unsafeCommit>>
Readd == /\ ~present /\ epoch < 2 /\ present' = TRUE /\ epoch' = epoch + 1
         /\ UNCHANGED <<allocated, staged, captured, marker, unsafeCommit>>
Commit(g) == /\ g \in staged /\ present
             /\ (~CheckEpoch \/ captured[g] = epoch)
             /\ (~CheckOrder \/ g >= marker)
             /\ marker' = g /\ staged' = staged \ {g}
             /\ unsafeCommit' = (unsafeCommit \/ captured[g] # epoch \/ g < marker)
             /\ UNCHANGED <<present, epoch, allocated, captured>>
Next == Allocate \/ Forget \/ Readd \/ (\E g \in 1..2 : Commit(g))
Spec == Init /\ [][Next]_vars
TypeOK == /\ present \in BOOLEAN /\ epoch \in 1..2 /\ allocated \in 0..2
          /\ staged \subseteq 1..2 /\ captured \in [1..2 -> 0..2]
          /\ marker \in 0..2 /\ unsafeCommit \in BOOLEAN
INV_RegistrationCannotCrossFence == ~unsafeCommit
====
