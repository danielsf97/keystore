package keystore;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Keystore {
    CompletableFuture<Boolean> put (Map<Long, byte[]> values) throws  Exception;

    CompletableFuture<Map <Long, byte[]>> get (Collection<Long> keys) throws Exception;
}
