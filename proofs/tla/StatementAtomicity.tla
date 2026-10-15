-------------------------- MODULE StatementAtomicity --------------------------
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    RestoreWrites,
    RestoreCallbacks,
    RestoreEvents,
    RestoreCreatedIds,
    RestoreAcpProjection,
    RecoverBeforeNextOperation,
    BlockCommitWhileCleanupPending

Statements == 1..2
Effects == Statements
States == {"Ready", "Open", "CancelledPending", "Committed"}

VARIABLES
    state,
    nextStatement,
    activeStatement,
    pendingStatement,
    cleanupPending,
    writes,
    callbacks,
    orderedEvents,
    createdIds,
    acpProjection,
    saveWrites,
    saveCallbacks,
    saveEvents,
    saveCreatedIds,
    saveAcpProjection,
    successful,
    aborted,
    committedWrites,
    committedCallbacks,
    committedEvents,
    committedCreatedIds,
    committedAcpProjection

vars == <<
    state,
    nextStatement,
    activeStatement,
    pendingStatement,
    cleanupPending,
    writes,
    callbacks,
    orderedEvents,
    createdIds,
    acpProjection,
    saveWrites,
    saveCallbacks,
    saveEvents,
    saveCreatedIds,
    saveAcpProjection,
    successful,
    aborted,
    committedWrites,
    committedCallbacks,
    committedEvents,
    committedCreatedIds,
    committedAcpProjection
>>

Init ==
    /\ state = "Ready"
    /\ nextStatement = 1
    /\ activeStatement = 0
    /\ pendingStatement = 0
    /\ cleanupPending = FALSE
    /\ writes = {}
    /\ callbacks = {}
    /\ orderedEvents = {}
    /\ createdIds = {}
    /\ acpProjection = {}
    /\ saveWrites = {}
    /\ saveCallbacks = {}
    /\ saveEvents = {}
    /\ saveCreatedIds = {}
    /\ saveAcpProjection = {}
    /\ successful = {}
    /\ aborted = {}
    /\ committedWrites = {}
    /\ committedCallbacks = {}
    /\ committedEvents = {}
    /\ committedCreatedIds = {}
    /\ committedAcpProjection = {}

OpenStatement ==
    /\ state = "Ready"
    /\ nextStatement \in Statements
    /\ activeStatement' = nextStatement
    /\ saveWrites' = writes
    /\ saveCallbacks' = callbacks
    /\ saveEvents' = orderedEvents
    /\ saveCreatedIds' = createdIds
    /\ saveAcpProjection' = acpProjection
    /\ pendingStatement' = 0
    /\ cleanupPending' = FALSE
    /\ state' = "Open"
    /\ UNCHANGED <<
        nextStatement, writes, callbacks, orderedEvents, createdIds,
        acpProjection, successful, aborted, committedWrites,
        committedCallbacks, committedEvents, committedCreatedIds,
        committedAcpProjection
    >>

StageStatementEffects ==
    /\ state = "Open"
    /\ writes' = writes \cup {activeStatement}
    /\ callbacks' = callbacks \cup {activeStatement}
    /\ orderedEvents' = orderedEvents \cup {activeStatement}
    /\ createdIds' = createdIds \cup {activeStatement}
    /\ acpProjection' = acpProjection \cup {activeStatement}
    /\ UNCHANGED <<
        state, nextStatement, activeStatement, pendingStatement,
        cleanupPending, saveWrites, saveCallbacks, saveEvents,
        saveCreatedIds, saveAcpProjection, successful, aborted,
        committedWrites, committedCallbacks, committedEvents,
        committedCreatedIds, committedAcpProjection
    >>

FinishStatement ==
    /\ state = "Open"
    /\ activeStatement \in writes
    /\ activeStatement \in callbacks
    /\ activeStatement \in orderedEvents
    /\ activeStatement \in createdIds
    /\ activeStatement \in acpProjection
    /\ successful' = successful \cup {activeStatement}
    /\ nextStatement' = activeStatement + 1
    /\ activeStatement' = 0
    /\ state' = "Ready"
    /\ UNCHANGED <<
        pendingStatement, cleanupPending, writes, callbacks, orderedEvents,
        createdIds, acpProjection, saveWrites, saveCallbacks, saveEvents,
        saveCreatedIds, saveAcpProjection, aborted, committedWrites,
        committedCallbacks, committedEvents, committedCreatedIds,
        committedAcpProjection
    >>

RestoreSavepoint ==
    /\ writes' = IF RestoreWrites THEN saveWrites ELSE writes
    /\ callbacks' = IF RestoreCallbacks THEN saveCallbacks ELSE callbacks
    /\ orderedEvents' = IF RestoreEvents THEN saveEvents ELSE orderedEvents
    /\ createdIds' = IF RestoreCreatedIds THEN saveCreatedIds ELSE createdIds
    /\ acpProjection' = IF RestoreAcpProjection THEN saveAcpProjection ELSE acpProjection

FailStatement ==
    /\ state = "Open"
    /\ aborted' = aborted \cup {activeStatement}
    /\ RestoreSavepoint
    /\ nextStatement' = activeStatement + 1
    /\ activeStatement' = 0
    /\ pendingStatement' = 0
    /\ cleanupPending' = FALSE
    /\ state' = "Ready"
    /\ UNCHANGED <<
        successful, saveWrites, saveCallbacks, saveEvents, saveCreatedIds,
        saveAcpProjection, committedWrites, committedCallbacks,
        committedEvents, committedCreatedIds, committedAcpProjection
    >>

CancelStatement ==
    /\ state = "Open"
    /\ aborted' = aborted \cup {activeStatement}
    /\ pendingStatement' = activeStatement
    /\ cleanupPending' = TRUE
    /\ state' = "CancelledPending"
    /\ UNCHANGED <<
        nextStatement, activeStatement, writes, callbacks, orderedEvents,
        createdIds, acpProjection, saveWrites, saveCallbacks, saveEvents,
        saveCreatedIds, saveAcpProjection, successful, committedWrites,
        committedCallbacks, committedEvents, committedCreatedIds,
        committedAcpProjection
    >>

RecoverCancellation ==
    /\ state = "CancelledPending"
    /\ cleanupPending
    /\ RestoreSavepoint
    /\ nextStatement' = pendingStatement + 1
    /\ activeStatement' = 0
    /\ pendingStatement' = 0
    /\ cleanupPending' = FALSE
    /\ state' = "Ready"
    /\ UNCHANGED <<
        successful, aborted, saveWrites, saveCallbacks, saveEvents,
        saveCreatedIds, saveAcpProjection, committedWrites,
        committedCallbacks, committedEvents, committedCreatedIds,
        committedAcpProjection
    >>

BeginWithoutRecovery ==
    /\ state = "CancelledPending"
    /\ ~RecoverBeforeNextOperation
    /\ pendingStatement < 2
    /\ activeStatement' = pendingStatement + 1
    /\ nextStatement' = pendingStatement + 1
    /\ saveWrites' = writes
    /\ saveCallbacks' = callbacks
    /\ saveEvents' = orderedEvents
    /\ saveCreatedIds' = createdIds
    /\ saveAcpProjection' = acpProjection
    /\ pendingStatement' = 0
    /\ cleanupPending' = FALSE
    /\ state' = "Open"
    /\ UNCHANGED <<
        writes, callbacks, orderedEvents, createdIds, acpProjection,
        successful, aborted, committedWrites, committedCallbacks,
        committedEvents, committedCreatedIds, committedAcpProjection
    >>

Commit ==
    /\ ((state = "Ready" /\ nextStatement = 3)
        \/ (state = "CancelledPending" /\ ~BlockCommitWhileCleanupPending))
    /\ committedWrites' = writes
    /\ committedCallbacks' = callbacks
    /\ committedEvents' = orderedEvents
    /\ committedCreatedIds' = createdIds
    /\ committedAcpProjection' = acpProjection
    /\ state' = "Committed"
    /\ UNCHANGED <<
        nextStatement, activeStatement, pendingStatement, cleanupPending,
        writes, callbacks, orderedEvents, createdIds, acpProjection,
        saveWrites, saveCallbacks, saveEvents, saveCreatedIds,
        saveAcpProjection, successful, aborted
    >>

Next ==
    \/ OpenStatement
    \/ StageStatementEffects
    \/ FinishStatement
    \/ FailStatement
    \/ CancelStatement
    \/ RecoverCancellation
    \/ BeginWithoutRecovery
    \/ Commit

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ state \in States
    /\ nextStatement \in 1..3
    /\ activeStatement \in 0..2
    /\ pendingStatement \in 0..2
    /\ cleanupPending \in BOOLEAN
    /\ writes \in SUBSET Effects
    /\ callbacks \in SUBSET Effects
    /\ orderedEvents \in SUBSET Effects
    /\ createdIds \in SUBSET Effects
    /\ acpProjection \in SUBSET Effects
    /\ saveWrites \in SUBSET Effects
    /\ saveCallbacks \in SUBSET Effects
    /\ saveEvents \in SUBSET Effects
    /\ saveCreatedIds \in SUBSET Effects
    /\ saveAcpProjection \in SUBSET Effects
    /\ successful \in SUBSET Effects
    /\ aborted \in SUBSET Effects
    /\ committedWrites \in SUBSET Effects
    /\ committedCallbacks \in SUBSET Effects
    /\ committedEvents \in SUBSET Effects
    /\ committedCreatedIds \in SUBSET Effects
    /\ committedAcpProjection \in SUBSET Effects

INV_AbortedEffectsRemoved ==
    \A statement \in aborted:
        (cleanupPending /\ pendingStatement = statement)
        \/ (statement \notin writes
            /\ statement \notin callbacks
            /\ statement \notin orderedEvents
            /\ statement \notin createdIds
            /\ statement \notin acpProjection)

INV_SuccessfulEffectsRetained ==
    /\ successful \subseteq writes
    /\ successful \subseteq callbacks
    /\ successful \subseteq orderedEvents
    /\ successful \subseteq createdIds
    /\ successful \subseteq acpProjection

INV_NoCommitWhileCleanupPending ==
    state = "Committed" => ~cleanupPending

INV_CommitPreservesOnlySuccessfulWork ==
    state = "Committed" =>
        /\ committedWrites = successful
        /\ committedCallbacks = successful
        /\ committedEvents = successful
        /\ committedCreatedIds = successful
        /\ committedAcpProjection = successful

=============================================================================
