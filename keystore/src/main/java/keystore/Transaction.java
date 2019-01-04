package keystore;

import io.atomix.utils.net.Address;

import tpc.TwoPCTransaction;

import java.util.HashMap;
import java.util.Map;

/**
 * Representa uma transação.
 *
 */

class Transaction extends TwoPCTransaction {

    // ***************************************************************************
    // Variáveis
    // ***************************************************************************

    private Map<Long, byte[]> keys;


    // ***************************************************************************
    // Construtores
    // ***************************************************************************

    /**
     * Construtor parametrizado de uma transação.
     * @param id
     * @param clientTxId
     * @param address
     */
    Transaction(Integer id, Integer clientTxId, Address address) {
        super(id, clientTxId, address);

        keys = new HashMap<>();
    }


    // ***************************************************************************
    // Getters e Setters
    // ***************************************************************************

    /**
     * Retorna as chaves de uma transação.
     *
     * @return as chaves de uma transação.
     */
    public Map<Long, byte[]> getKeys() {
        return keys;
    }


    /**
     * Adiciona um conjunto de chaves à transação.
     *
     * @param keys Chaves a adicionar à transação.
     */
    public void setKeys(Map<Long, byte[]> keys) {
        keys.putAll(keys);
    }
}