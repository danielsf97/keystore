package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.security.Key;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Server {
    public static void main(String[] args) throws Exception {

        Keystore bs = new KeystoreSrv();
        Map<Integer, Transaction> currentTransactions;

        Serializer s = KeystoreProtocol.newSerializer();
        ManagedMessagingService ms = NettyMessagingService.builder().withAddress(Address.from(12345)).build();

        ms.registerHandler("put", (c,m)-> {
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
        });

        ms.start().get();
    }
}
