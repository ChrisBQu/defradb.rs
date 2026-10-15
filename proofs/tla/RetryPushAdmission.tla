---- MODULE RetryPushAdmission ----
\* A queued job must still own a live durable marker when it is admitted.
\* The snapshot check is the boundary; already admitted I/O is not revoked.
EXTENDS Naturals
CONSTANT CheckPresence, CheckGeneration, CheckDeadline
ASSUME /\ CheckPresence \in BOOLEAN /\ CheckGeneration \in BOOLEAN /\ CheckDeadline \in BOOLEAN
VARIABLES peer, pending, generation, queued, observed, blocked, unsafeSend
vars == <<peer, pending, generation, queued, observed, blocked, unsafeSend>>
Init == /\ peer = TRUE /\ pending = FALSE /\ generation = 0
        /\ queued = FALSE /\ observed = 0 /\ blocked = FALSE /\ unsafeSend = FALSE
Register == /\ peer /\ generation < 2 /\ generation' = generation + 1 /\ pending' = TRUE
            /\ UNCHANGED <<peer, queued, observed, blocked, unsafeSend>>
Enqueue == /\ peer /\ pending /\ ~queued /\ queued' = TRUE /\ observed' = generation
           /\ UNCHANGED <<peer, pending, generation, blocked, unsafeSend>>
Forget == /\ peer /\ peer' = FALSE /\ pending' = FALSE
          /\ UNCHANGED <<generation, queued, observed, blocked, unsafeSend>>
Readd == /\ ~peer /\ peer' = TRUE
         /\ UNCHANGED <<pending, generation, queued, observed, blocked, unsafeSend>>
Complete == /\ pending /\ pending' = FALSE
            /\ UNCHANGED <<peer, generation, queued, observed, blocked, unsafeSend>>
Backpressure == /\ blocked' = ~blocked
                /\ UNCHANGED <<peer, pending, generation, queued, observed, unsafeSend>>
Admit == /\ queued
         /\ (~CheckPresence \/ (peer /\ pending))
         /\ (~CheckGeneration \/ observed = generation)
         /\ (~CheckDeadline \/ ~blocked)
         /\ queued' = FALSE
         /\ unsafeSend' = (unsafeSend \/ ~peer \/ ~pending \/ observed # generation \/ blocked)
         /\ UNCHANGED <<peer, pending, generation, observed, blocked>>
Next == Register \/ Enqueue \/ Forget \/ Readd \/ Complete \/ Backpressure \/ Admit
Spec == Init /\ [][Next]_vars
TypeOK == /\ peer \in BOOLEAN /\ pending \in BOOLEAN /\ generation \in 0..2
          /\ queued \in BOOLEAN /\ observed \in 0..2 /\ blocked \in BOOLEAN /\ unsafeSend \in BOOLEAN
INV_QueuedPushOwnsLiveMarker == ~unsafeSend
====
