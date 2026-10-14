---- MODULE TransactionDecisions ----
\* A document comparison/deletion/definition decision must be validated by
\* the transaction that writes its effects, including changes before it begins.
EXTENDS Naturals
CONSTANT Inside
VARIABLES phase, version, observed, beginVersion, staleCommit
vars == <<phase, version, observed, beginVersion, staleCommit>>
Init == /\ phase = "new" /\ version = 0 /\ observed = 0
        /\ beginVersion = 0 /\ staleCommit = FALSE
Read == /\ phase = "new"
        /\ observed' = version
        /\ beginVersion' = version
        /\ phase' = IF Inside THEN "ready" ELSE "read"
        /\ UNCHANGED <<version, staleCommit>>
Begin == /\ phase = "read" /\ phase' = "ready"
         /\ beginVersion' = version
         /\ UNCHANGED <<version, observed, staleCommit>>
ConcurrentChange == /\ version = 0 /\ version' = 1
                    /\ UNCHANGED <<phase, observed, beginVersion, staleCommit>>
Commit == /\ phase = "ready" /\ phase' = "done"
          /\ staleCommit' = (version = beginVersion) /\ (observed # version)
          /\ UNCHANGED <<version, observed, beginVersion>>
Next == Read \/ Begin \/ ConcurrentChange \/ Commit
Spec == Init /\ [][Next]_vars
DecisionsValidated == ~staleCommit
====
