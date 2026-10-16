# Explicit transaction statement atomicity

An explicit GraphQL mutation request can contain several top-level statements
inside one transaction. A successful earlier statement remains staged if a
later statement fails; effects from the failed statement must be removed before
the next operation or commit. The savepoint therefore covers the Regolith
transaction, adapter callbacks and ordered events, DB-created-ID/cache state,
and the attempt-local ACP registration projection.

Cancellation is different from an ordinary returned error: dropping the
statement guard only marks cleanup pending. The next transaction operation
under the existing action lock restores the saved state before proceeding. A
commit also performs that recovery first. The model treats those lock-ordered
operations as serial transitions; it adds no lock or concurrent savepoint.

`StatementAtomicity.tla` models two statements. It checks that failure restores
all staged effects, cancellation cleanup retains a previously successful
statement while removing the cancelled one, and no next operation or commit
can cross pending cleanup. RED configurations skip storage rollback, omit a
side-effect snapshot, or bypass cleanup before a next operation/commit.

This is a bounded protocol abstraction. It does not prove savepoint byte
fidelity, cancellation behavior in Rust futures, or every transaction entry
point; the implementation must preserve the listed state in the actual
savepoint APIs and route all operations through the recovery check.
