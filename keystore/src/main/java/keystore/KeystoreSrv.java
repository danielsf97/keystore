package keystore;

import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class KeystoreSrv implements Keystore {

    Map<Long, byte[]> data;

    public KeystoreSrv(){
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
}