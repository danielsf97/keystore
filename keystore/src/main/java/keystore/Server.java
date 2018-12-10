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
    private Log log;

    private HashMap<Integer, Transaction> currentTransactions;

    public Server() {

        this.ms =  NettyMessagingService.builder()
                .withAddress(myAddress)
                .build();

        this.s = KeystoreProtocol
                .newSerializer();

        this.es = Executors.newSingleThreadExecutor();

        this.currentTransactions = new HashMap<>();

        this.ms.registerHandler("put", (c,m)-> {
            KeystoreProtocol.PutReq r = s.decode(m);

            Map<Long,byte[]> values = r.values;
            CompletableFuture<byte[]> cf = new CompletableFuture<>();
            try {
                CompletableFuture<Boolean> ok = bs.put(r.values);
                cf.complete(s.encode(new KeystoreProtocol.PutRep(ok)));
            } catch (Exception e) {
                cf.completeExceptionally(e);
            }
            return cf;
        });

        this.ms.registerHandler("get", (c, m)-> {
            KeystoreProtocol.GetReq r = s.decode(m);

            CompletableFuture<byte[]> cf = new CompletableFuture<>();
            try {
                CompletableFuture<Map<Long,byte[]>> ok = bs.get(r.keys);
                cf.complete(s.encode(new KeystoreProtocol.GetRep(ok)));
            } catch (Exception e) {
                cf.completeExceptionally(e);
            }
            return cf;
        });

    }




    public static void main(String[] args) throws Exception {
        Server server = new Server();

        ms.start().get();
    }
}
