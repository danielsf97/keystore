package tpc;


/**
 * Fases nas quais uma transação se pode encontrar.
 *
 */
public enum Phase {
    STARTED, PREPARED, COMMITTED, ABORT, ROLLBACKED
}