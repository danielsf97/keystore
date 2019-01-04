package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import tpc.Participant;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;


public class KeystoreSrv {

    private static final Address[] addresses = new Address[] {
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };

    private Map<Long, LinkedList<CompletableFuture<Void>>> keyLocks;
    private ReentrantLock keyLocksGlobalLock;
    private Map<Long, Boolean> keyBusy;
    private ManagedMessagingService ms;

    private final Map<Long, byte[]> data;
    private static Serializer s;

    private KeystoreSrv(int id) {
        ExecutorService es = Executors.newFixedThreadPool(3);
        this.ms = NettyMessagingService.builder()
                .withAddress(addresses[id])
                .build();

        this.data = new HashMap<>();
        this.keyLocks = new HashMap<>();
        this.keyBusy = new HashMap<>();
        this.keyLocksGlobalLock = new ReentrantLock();

        s = ServerKeystoreSrvProtocol
                .newSerializer();

        Function<Map<Long, byte[]>,CompletableFuture<Void>> prepare = keys -> getLocks(new TreeSet<>(keys.keySet()));

        Consumer<Map<Long, byte[]>> commit = keys -> {
            synchronized (data) {
                data.putAll(keys);
            }
            releaseLocks(keys);
        };

        Consumer<Map<Long,byte[]>> abort = this::releaseLocksAbort;
        new Participant<>(id, ms, es, "KeyStoreSrv" + id, prepare, commit, abort);
        ms.registerHandler(ServerKeystoreSrvProtocol.GetControllerReq.class.getName(), this::get, es);
        ms.start();
    }




    private void get(Address address, byte[] m) {
        ServerKeystoreSrvProtocol.GetControllerReq prepReq = s.decode(m);
        Collection<Long> keys = prepReq.keys;
        Map<Long,byte[]> rp = new HashMap<>();
        for(Long e : keys) {
            synchronized (data) {
                if (data.containsKey(e))
                    rp.put(e, data.get(e));
            }
        }
        ServerKeystoreSrvProtocol.GetControllerResp resp = new ServerKeystoreSrvProtocol.GetControllerResp(prepReq.txId, prepReq.pId,rp);
        ms.sendAsync(address, ServerKeystoreSrvProtocol.GetControllerResp.class.getName(),s.encode(resp));
    }


    private CompletableFuture<Void> getLocks(TreeSet<Long> keys) {

        this.keyLocksGlobalLock.lock();

        CompletableFuture[] ready = new CompletableFuture[keys.size()];
        int i = 0;

        for(Long keyId: keys) {
            if(keyBusy.containsKey(keyId) && keyBusy.get(keyId)) {
                CompletableFuture<Void> cf = new CompletableFuture<>();
                cf.thenRun(()-> keyBusy.put(keyId,true));
                LinkedList<CompletableFuture<Void>> q = keyLocks.get(keyId);
                q.add(cf);
                keyLocks.put(keyId,q);
                ready[i] = cf;
            }
            else if (keyBusy.containsKey(keyId) && !keyBusy.get(keyId)) {
                keyBusy.put(keyId,true);
                ready[i] = CompletableFuture.completedFuture(null);
            }
            else {
                keyBusy.put(keyId,true);
                keyLocks.put(keyId, new LinkedList<>());
                ready[i] = CompletableFuture.completedFuture(null);
            }
            i++;
        }

        this.keyLocksGlobalLock.unlock();

        return CompletableFuture.allOf(ready);
    }


    private void releaseLocksAbort(Map<Long,byte[]> keys) {

        this.keyLocksGlobalLock.lock();

        for(Long keyId: keys.keySet()) {
            LinkedList<CompletableFuture<Void>> q = keyLocks.get(keyId);

            if(q != null) {
                if(q.isEmpty()) {

                    keyBusy.put(keyId, false);
                    boolean dataContainsKey;

                    synchronized (data) {
                        dataContainsKey = data.containsKey(keyId);
                    }

                    if (!dataContainsKey) {
                        keyLocks.remove(keyId);
                        keyBusy.remove(keyId);
                    }
                }
                else {
                    q.removeFirst().complete(null);
                }
            }
        }
        this.keyLocksGlobalLock.unlock();
    }

    private void releaseLocks(Map<Long,byte[]> keys) {
        this.keyLocksGlobalLock.lock();

        for(Long keyId: keys.keySet()) {
            LinkedList<CompletableFuture<Void>> q = keyLocks.get(keyId);
            if(q != null) {
                if (q.isEmpty()) {
                    keyBusy.put(keyId, false);
                }
                else {
                    q.removeFirst().complete(null);
                }
            }
        }
        this.keyLocksGlobalLock.unlock();

    }

    public static void main(String[] args) {
        int id = Integer.parseInt(args[0]);
        new KeystoreSrv(id);
    }

}