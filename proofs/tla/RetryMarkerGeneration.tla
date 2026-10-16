---- MODULE RetryMarkerGeneration ----
\* A retry marker's generation fences an acknowledgement that was computed
\* before a newer registration. Peer presence is the incarnation fence used
\* by clear/re-add paths. Abstraction of peerstore.rs transactions.
EXTENDS Naturals
CONSTANT CompareGeneration, ValidatePeer
ASSUME CompareGeneration \in BOOLEAN
ASSUME ValidatePeer \in BOOLEAN
VARIABLES present, incarnation, marker, observed, pending, clearEpoch, clearing, staleClear
vars == <<present, incarnation, marker, observed, pending, clearEpoch, clearing, staleClear>>
Init == /\ present = TRUE /\ incarnation = 0 /\ marker = 0 /\ observed = 0 /\ pending = TRUE
        /\ clearEpoch = 0 /\ clearing = FALSE /\ staleClear = FALSE
\* Every registration advances the token, even when the scope was already dirty.
Register == /\ present /\ marker < 2 /\ marker' = marker + 1 /\ pending' = TRUE
           /\ UNCHANGED <<present, incarnation, observed, clearEpoch, clearing, staleClear>>
\* A worker records the generation it observed before doing its external work.
Observe == /\ pending /\ observed' = marker /\ UNCHANGED <<present, incarnation, marker, pending, clearEpoch, clearing, staleClear>>
\* Ack must compare the observed token so it cannot erase a newer registration.
Ack == /\ pending
       /\ marker' = IF CompareGeneration /\ observed # marker THEN marker ELSE marker
       /\ pending' = IF CompareGeneration /\ observed # marker THEN pending ELSE FALSE
       /\ UNCHANGED <<present, incarnation, observed, clearEpoch, clearing, staleClear>>
\* Forget and re-add change the peer incarnation and install fresh dirty state.
Forget == /\ present /\ incarnation < 2 /\ present' = FALSE /\ incarnation' = incarnation + 1
         /\ UNCHANGED <<marker, observed, pending, clearEpoch, clearing, staleClear>>
Readd == /\ ~present /\ incarnation < 2 /\ marker < 2
         /\ present' = TRUE /\ incarnation' = incarnation + 1
         /\ marker' = marker + 1 /\ pending' = TRUE /\ UNCHANGED <<observed, clearEpoch, clearing, staleClear>>
\* Clear records the peer incarnation before it sweeps retry state.
BeginClear == /\ present /\ ~clearing /\ clearing' = TRUE
              /\ clearEpoch' = incarnation
              /\ UNCHANGED <<present, incarnation, marker, observed, pending, staleClear>>
\* A valid clear can finish only for the same incarnation; RED records stale completion.
FinishClear == /\ clearing /\ clearing' = FALSE
               /\ IF ValidatePeer
                    THEN /\ clearEpoch = incarnation /\ pending' = FALSE
                         /\ observed' = marker /\ staleClear' = staleClear
                    ELSE /\ pending' = FALSE /\ observed' = marker
                         /\ staleClear' = (staleClear \/ (clearEpoch # incarnation))
               /\ UNCHANGED <<present, incarnation, marker, clearEpoch>>
Next == Register \/ Observe \/ Ack \/ Forget \/ Readd \/ BeginClear \/ FinishClear
Spec == Init /\ [][Next]_vars
TypeOK == /\ present \in BOOLEAN /\ incarnation \in Nat /\ marker \in Nat
          /\ observed \in Nat /\ pending \in BOOLEAN /\ clearEpoch \in Nat
          /\ clearing \in BOOLEAN /\ staleClear \in BOOLEAN
\* Pending work or equal generations ensure an old ack never removed new work.
INV_NewRegistrationSurvivesOldAck == pending \/ marker = observed
\* A clear crossing forget/re-add is rejected; staleClear is the mutant witness.
INV_NewRegistrationSurvivesOldClear == ~staleClear
====
