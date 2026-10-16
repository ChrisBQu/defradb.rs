---- MODULE CollectionVersionOCC ----
\* Structural schema writes invalidate a document writer that selected an
\* obsolete Collection version before its transaction attempt.
EXTENDS Naturals
CONSTANT ValidateVersion
ASSUME ValidateVersion \in BOOLEAN
VARIABLES phase, schemaVersion, observedVersion, staleWrite
vars == <<phase, schemaVersion, observedVersion, staleWrite>>
Init == /\ phase = "new" /\ schemaVersion = 0
        /\ observedVersion = 0 /\ staleWrite = FALSE
\* The writer selects its collection version inside its transaction attempt.
ReadCollection == /\ phase = "new" /\ phase' = "active"
                  /\ observedVersion' = schemaVersion
                  /\ UNCHANGED <<schemaVersion, staleWrite>>
\* A structural collection update publishes a new active schema version.
StructuralPut == /\ schemaVersion = 0 /\ schemaVersion' = 1
                 /\ UNCHANGED <<phase, observedVersion, staleWrite>>
\* A valid writer commits only if its selected version still matches current state.
Commit == /\ phase = "active" /\ (~ValidateVersion \/ observedVersion = schemaVersion)
          /\ phase' = "done"
          /\ staleWrite' = (observedVersion # schemaVersion) /\ UNCHANGED <<schemaVersion, observedVersion>>
\* Validation aborts the stale writer rather than applying its old-schema decision.
RejectStale == /\ ValidateVersion /\ phase = "active"
               /\ observedVersion # schemaVersion /\ phase' = "aborted"
               /\ UNCHANGED <<schemaVersion, observedVersion, staleWrite>>
Next == ReadCollection \/ StructuralPut \/ Commit \/ RejectStale
Spec == Init /\ [][Next]_vars
TypeOK == /\ phase \in {"new", "active", "done", "aborted"}
          /\ schemaVersion \in 0..1 /\ observedVersion \in 0..1
          /\ staleWrite \in BOOLEAN
\* No completed write may have relied on a schema version superseded before commit.
INV_NoStaleSchemaWrite == ~staleWrite
====
