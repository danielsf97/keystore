package keystore;

import com.sun.org.apache.xpath.internal.operations.Bool;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import org.graalvm.compiler.nodes.memory.address.AddressNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KeystoreSrv implements Keystore {


    private static final Address[] addresses = {
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };

    Map<Long, byte[]> data;
    private static ManagedMessagingService ms;
    private static Serializer s;
    private ExecutorService es;
    private Log log;

    int id;

    public KeystoreSrv() {

    }

    public KeystoreSrv(int id){

        this.id = id;

        this.ms = NettyMessagingService.builder()
                .withAddress(addresses[id])
                .build();

        this.s = KeystoreProtocol
                .newSerializer();

        this.es = Executors.newSingleThreadExecutor();

        this.log = new Log();

        data = new HashMap<>();
    }


    @Override
    public CompletableFuture<Boolean> put(Map<Long, byte[]> values) throws Exception {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> keys) throws Exception {
        return CompletableFuture.completedFuture(new HashMap<>());
    }


/*        ms.registerHandler("put", (c,m)-> {
        KeystoreProtocol.PutReq r = s.decode(m);

        CompletableFuture<byte[]> cf = new CompletableFuture<>();
        try {
            CompletableFuture<Boolean> ok = bs.put(r.values);
            cf.complete(s.encode(new KeystoreProtocol.PutRep(ok)));
        } catch (Exception e) {
            cf.completeExceptionally(e);
        }
        return cf;
    });

        ms.registerHandler("get", (c,m)-> {
        KeystoreProtocol.GetReq r = s.decode(m);

        CompletableFuture<byte[]> cf = new CompletableFuture<>();
        try {
            CompletableFuture<Map<Long,byte[]>> ok = bs.get(r.keys);
            cf.complete(s.encode(new KeystoreProtocol.GetRep(ok)));
        } catch (Exception e) {
            cf.completeExceptionally(e);
        }
        return cf;
    });*/


}