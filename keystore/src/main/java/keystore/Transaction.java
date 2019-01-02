package keystore;

import io.atomix.utils.net.Address;

import keystore.tpc.TwoPCTransaction;

import java.util.HashMap;
import java.util.Map;

class Transaction extends TwoPCTransaction {

    private Map<Long,byte[]> keys;

    Transaction(Integer id, Integer client_txId, Address address){

        super(id,client_txId,address);
        keys = new HashMap<>();

    }

    public Map<Long,byte[]> getKeys (){
        return keys;
    }

    public void setKeys(Map<Long,byte[]> chaves){
        System.out.println("A colocar chaves");
        keys.putAll(chaves);
    }
}