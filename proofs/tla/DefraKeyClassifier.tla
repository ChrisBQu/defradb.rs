---- MODULE DefraKeyClassifier ----
\* Two transactions begin with an absent key. Content-addressed blocks may
\* both land. A marker read used to decide a side effect must have one winner.
\* This models the caller's classification contract, not engine internals.
EXTENDS Naturals, FiniteSets
CONSTANT RelaxMarkers
VARIABLES committed, present, effects
vars == <<committed, present, effects>>
Kinds == {"block", "marker"}
Writers == {1, 2}
Init == /\ committed = [k \in Kinds |-> {}]
        /\ present = [k \in Kinds |-> FALSE]
        /\ effects = 0
Commit(k, w) ==
    /\ w \notin committed[k]
    /\ ~present[k] \/ k = "block" \/ RelaxMarkers
    /\ committed' = [committed EXCEPT ![k] = @ \cup {w}]
    /\ present' = [present EXCEPT ![k] = TRUE]
    /\ effects' = IF k = "marker" THEN effects + 1 ELSE effects
Next == \E k \in Kinds, w \in Writers : Commit(k, w)
Spec == Init /\ [][Next]_vars
OneMarkerWinner == effects <= 1
BothBlocksReachable == Cardinality(committed["block"]) < 2
====
