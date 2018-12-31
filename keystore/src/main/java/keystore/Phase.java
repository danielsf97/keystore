package keystore;


/**
 * Fases nas quais uma transação se pode encontrar.
 *
 */
public enum Phase {
    STARTED, PREPARED, COMMITED, ABORT, ROLLBACKED
}