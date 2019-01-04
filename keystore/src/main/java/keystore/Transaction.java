package keystore;

import io.atomix.utils.net.Address;

import tpc.TwoPCTransaction;

import java.util.HashMap;
import java.util.Map;

class Transaction extends TwoPCTransaction {

    private Map<Long,byte[]> keys;

    Transaction(Integer id, Integer clientTxId, Address address) {
        super(id, clientTxId, address);

        keys = new HashMap<>();

    }

    public Map<Long,byte[]> getKeys() {
        return keys;
    }

    public void setKeys(Map<Long,byte[]> keys) {
        keys.putAll(keys);
    }
}