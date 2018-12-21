package keystore;

import io.atomix.utils.net.Address;
import java.util.Map;

public class SimpleTransaction {
    Integer id;
    Integer clientId;
    Integer clientAddress;
    Map<Integer, Map<Long,byte[]>> participantsToKeys;

    public SimpleTransaction(Integer id, Integer  clientId, Address  clientAddress, Map<Integer, Map<Long,byte[]>>  participantsToKeys){
        this.id = id;
        this.clientId = clientId;
        this.clientAddress = clientAddress.port();
        this.participantsToKeys = participantsToKeys;
    }
}
