package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import spullara.nio.channels.FutureServerSocketChannel;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class KeystoreCli implements Keystore {
    private final ManagedMessagingService ms;
    private final Address srv;
    private final Serializer s;

    public KeystoreCli(int port ) throws Exception {
        srv = Address.from("localhost", port);
        ms = NettyMessagingService.builder().withAddress(Address.from(10000)).build();
        s = KeystoreProtocol.newSerializer();
        ms.start().get();
    }

    @Override
    public CompletableFuture<Boolean> put(Map<Long, byte[]> values) throws Exception {
        KeystoreProtocol.PutReq req = new KeystoreProtocol.PutReq(values);
        CompletableFuture<byte[]> r = ms.sendAndReceive(srv, "put", s.encode(req));

        return r.thenApply((msg) -> {
            KeystoreProtocol.PutResp rep = s.decode(msg);
            return rep.state;
        });
    }

    @Override
    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> keys) throws Exception {
        KeystoreProtocol.GetReq req = new KeystoreProtocol.GetReq(keys);
        CompletableFuture<byte[]> r = ms.sendAndReceive(srv, "get", s.encode(req));
        return r.thenApply((msg) -> {
            KeystoreProtocol.GetResp resp = s.decode(msg);
            return resp.values;
        });
    }
}
