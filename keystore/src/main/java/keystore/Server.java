package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Server {

    private static final Address[] addresses = new Address[] {
            Address.from("localhost", 12345),
            Address.from("localhost", 12346),
            Address.from("localhost", 12347)
    };



    private final Address myAddress = Address.from("localhost:12350");
    private static ManagedMessagingService ms;
    private static Serializer s;
    private ExecutorService es;
    private keystore.Coordinator coordinator;

    private HashMap<Integer, Transaction> currentTransactions;

    public Server() {

        this.s = KeystoreProtocol
                .newSerializer();

        this.es = Executors.newSingleThreadExecutor();

        this.ms =  NettyMessagingService.builder()
                .withAddress(myAddress)
                .build();

        this.ms.registerHandler("put", (c, m) ->{
            KeystoreProtocol.PutReq req = s.decode(m);
            CompletableFuture<byte[]> cf = new CompletableFuture<>();

            boolean success = processPutReq(req);

            cf.complete(s.encode(new KeystoreProtocol.PutResp(success)));
            return cf;
        });

        this.coordinator = new Coordinator(this.ms, this.es, addresses);

        this.ms.start();
    }

    private boolean processPutReq(KeystoreProtocol.PutReq req) {
        Map<Long,byte[]> values = req.values;
        Map<Integer, Map<Long, byte []>> separatedValues = valuesSeparator(values);
        return coordinator.twoPhaseCommit(separatedValues);
    }

    private Map<Integer, Map<Long,byte[]>> valuesSeparator(Map<Long,byte[]> values) {
        Map<Integer, Map<Long, byte []>> res = new HashMap<>();
        for(Map.Entry value: values.entrySet()){
            int ks = (int) ((long) value.getKey() % (long) addresses.length);
            System.out.println("Participante " + ks);
            if(!res.containsKey(ks)) res.put(ks, new HashMap<>());
            Map ksMap = res.get(ks);
            ksMap.put(value.getKey(), value.getValue());
            res.put(ks, ksMap);
        }
        return res;
    }


    public static void main(String[] args) throws Exception {
        Server server = new Server();
    }
}
