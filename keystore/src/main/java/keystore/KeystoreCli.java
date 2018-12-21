package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class KeystoreCli implements Keystore {
    private final ManagedMessagingService ms;
    private final Address srv;
    private final Serializer s;
    private final Map<Integer,CompletableFuture<Boolean>> put_requests;
    private final Map<Integer,CompletableFuture<Map<Long, byte[]>>> get_requests;
    private  int i;
    private final ExecutorService es;
    KeystoreCli(int port) throws Exception {
        i = 0;
        es = Executors.newSingleThreadExecutor();
        srv = Address.from("localhost", port);
        ms = NettyMessagingService.builder().withAddress(Address.from(10000)).build();
        s = KeystoreProtocol.newSerializer();

        put_requests = new HashMap<>();
        get_requests = new HashMap<>();

        ms.registerHandler(KeystoreProtocol.PutResp.class.getName(),(o,m)->{
            KeystoreProtocol.PutResp ooo = s.decode(m);
            System.out.println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
            put_requests.remove(ooo.txId).complete(ooo.state);
        },es);

        ms.registerHandler(KeystoreProtocol.GetResp.class.getName(),(o,m)->{
            KeystoreProtocol.GetResp ooo = s.decode(m);
            System.out.println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
            get_requests.remove(ooo.txId).complete(ooo.values);
        },es);

        ms.start().get();
    }

    @Override
    public CompletableFuture<Boolean> put(Map<Long, byte[]> values) {

        KeystoreProtocol.PutReq req = new KeystoreProtocol.PutReq(values,i);
        ms.sendAsync(srv,"put",s.encode(req));

        CompletableFuture<Boolean> cp = new CompletableFuture<>();
        put_requests.put(i,cp);

        i++;

        return cp;


    }

    @Override
    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> keys) {
        KeystoreProtocol.GetReq req = new KeystoreProtocol.GetReq(keys,i);
        ms.sendAsync(srv,"get",s.encode(req));

        CompletableFuture<Map<Long, byte[]>> cp = new CompletableFuture<>();
        get_requests.put(i,cp);

        i++;

        return cp;
    }
}
