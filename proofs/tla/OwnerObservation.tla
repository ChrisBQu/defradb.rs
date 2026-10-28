-------------------------- MODULE OwnerObservation --------------------------
\* A row and its owner must belong to one authorized transaction snapshot.
\* The Rust owner is QueryRunner::observe_owners.
EXTENDS Naturals
\* A RED configuration skips one gate, or reads an owner outside the snapshot.
CONSTANT Fault
\* The request chooses one relevant authorization/membership condition.
Scenarios == {"allowed", "actor", "node", "app", "document", "member", "registered", "bound"}
\* The live owner may change after the reader's transaction begins.
VARIABLES scenario, phase, liveOwner, snapshotOwner, liveRegistered, snapshotRegistered, returned, observedOwner, visible
\* One tuple names every variable for stuttering.
vars == <<scenario, phase, liveOwner, snapshotOwner, liveRegistered, snapshotRegistered, returned, observedOwner, visible>>
\* Begin from a captured owner and no released result.
Init == /\ scenario \in Scenarios
        /\ phase = "ready"
        /\ liveOwner = 0
        /\ snapshotOwner = 0
        /\ snapshotRegistered = (scenario # "registered")
        /\ liveRegistered = snapshotRegistered
        /\ returned = FALSE
        /\ observedOwner = 2
        /\ visible = FALSE
\* Another transaction changes ownership without changing our snapshot.
ChangeOwner == /\ liveOwner = 0
               /\ liveOwner' = 1
               /\ UNCHANGED <<scenario, phase, snapshotOwner, liveRegistered, snapshotRegistered, returned, observedOwner, visible>>
\* Outside registration commits must not alter the captured view.
ChangeRegistration == /\ liveRegistered' = ~liveRegistered
                      /\ UNCHANGED <<scenario, phase, liveOwner, snapshotOwner, snapshotRegistered, returned, observedOwner, visible>>
\* The ordinary read path owns authorization and live document membership.
Readable == scenario \in {"allowed", "registered"} \/ scenario = Fault
\* Registration is read from the same view as the document and owner.
Registration == IF Fault = "registered" THEN TRUE
                ELSE IF Fault = "registration_snapshot" THEN liveRegistered
                ELSE snapshotRegistered
Eligible == Readable /\ Registration
\* The GREEN read uses the captured view; the snapshot fault uses live ACP.
Read == /\ phase = "ready"
        /\ phase' = "done"
        /\ returned' = Eligible
        \* Public reads remain visible even when ownership is not registered.
        /\ visible' = IF Fault = "visibility_registered" THEN Eligible ELSE Readable
        /\ observedOwner' = IF Eligible THEN
                (IF Fault = "snapshot" THEN liveOwner ELSE snapshotOwner) ELSE 2
        /\ UNCHANGED <<scenario, liveOwner, snapshotOwner, liveRegistered, snapshotRegistered>>
\* Explore the outside commit before or after the metadata read.
Next == ChangeOwner \/ ChangeRegistration \/ Read
\* The operation may finish or stutter; it creates no durable writes.
Spec == Init /\ [][Next]_vars
\* An inaccessible or unbound document never releases an owner.
NoUnauthorizedOwner == returned => scenario = "allowed"
\* Every released owner belongs to the same view as the document row.
SameSnapshot == returned => observedOwner = snapshotOwner
\* A concurrent registration or removal cannot add or hide a captured owner.
SameRegistrationSnapshot == (phase = "done" /\ Readable) => (returned = snapshotRegistered)
\* Read denial never exposes whether a hidden or missing document exists.
NoUnauthorizedVisibility == visible => scenario \in {"allowed", "registered"}
\* Unknown ownership must not be reported as failed read authorization.
VisibilityMatchesRead == phase = "done" => visible = Readable
=============================================================================
