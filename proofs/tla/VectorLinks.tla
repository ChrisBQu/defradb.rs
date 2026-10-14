---- MODULE VectorLinks ----
\* HNSW back-links are set additions; pruning is a validated whole-node put.
\* Search may use an advisory snapshot. A stale pruning decision must not
\* overwrite a link added since that snapshot. This models one node/layer;
\* vector/deleted/height changes are whole-value terminators under the same rule.
EXTENDS Naturals, FiniteSets
CONSTANTS Writers, BlindAdds, ValidatePrune
VARIABLES links, committed, version, pruneVersion, pruneLinks, pruned, pruneState
vars == <<links, committed, version, pruneVersion, pruneLinks, pruned, pruneState>>
Init == /\ links = {} /\ committed = {} /\ version = 0
        /\ pruneVersion = 0 /\ pruneLinks = {} /\ pruned = {} /\ pruneState = "new"
\* Both writers searched the initially empty layer. Blind merges add to the
\* current value; a replacement writes that old empty snapshot plus its link.
Add(w) == /\ w \notin committed
          /\ links' = IF BlindAdds THEN links \cup {w} ELSE {w}
          /\ committed' = committed \cup {w}
          /\ version' = version + 1
          /\ UNCHANGED <<pruneVersion, pruneLinks, pruned, pruneState>>
ObservePrune == /\ pruneState = "new"
                /\ pruneVersion' = version /\ pruneLinks' = links
                /\ pruneState' = "observed"
                /\ UNCHANGED <<links, committed, version, pruned>>
Prune == /\ pruneState = "observed"
         /\ IF ValidatePrune /\ version # pruneVersion
            THEN /\ UNCHANGED <<links, version, pruned>>
            ELSE /\ links' = {} /\ version' = version + 1 /\ pruned' = pruneLinks
         /\ pruneState' = "done"
         /\ UNCHANGED <<committed, pruneVersion, pruneLinks>>
Next == (\E w \in Writers: Add(w)) \/ ObservePrune \/ Prune
Spec == Init /\ [][Next]_vars
INV_NoLostLinks == committed = links \cup pruned
====
