package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class Coordinator {

    private Address [] addresses;
    private ExecutorService es;
    private ManagedMessagingService ms;
    private Serializer s;
    private Log log;
    private static AtomicInteger nextTxId = new AtomicInteger(0);

    private Map<Integer, Transaction> transactions = new HashMap<>();


    public Coordinator(ManagedMessagingService ms, ExecutorService es, Address[] addresses) {
        this.addresses = addresses;
        this.s = TwoFCProtocol.newSerializer();
        this.es = es;
        this.ms = ms;

        ms.registerHandler(TwoFCProtocol.ControllerPreparedResp.class.getName(), (c, m) ->{

        }, es);
    }

    public boolean twoPhaseCommit(Map<Integer, Map<Long,byte[]>> separatedValues) {
        int txId = nextTxId.incrementAndGet();

        for(Map.Entry<Integer, Map<Long, byte[]>> ksValues: separatedValues.entrySet()){
            KeystoreProtocol.ControllerPreparedReq contReq = new KeystoreProtocol.ControllerPreparedReq(txId, ksValues.getValue());
            CompletableFuture<byte[]> completableFuture = ms.sendAndReceive(addresses[(int) ksValues.getKey()],
                    KeystoreProtocol.ControllerPreparedReq.class.getName(),
                    s.encode(contReq));

        }

    }
    
}
