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


    private static final Address[] addresses = new Address[]{
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };

    private final Map<Long, byte[]> data;
    private Map<Long, LinkedList<CompletableFuture<Void>>> keyLocks;
    private ReentrantLock keyLocksGlobalLock;
    private Map<Long, Boolean> keyBusy;

    private static Serializer s;
    private ManagedMessagingService ms;


    private KeystoreSrv(int id) {
        ExecutorService es = Executors.newFixedThreadPool(3);
        ms = NettyMessagingService.builder()
                .withAddress(addresses[id])
                .build();

        this.data = new HashMap<>();
        this.keyLocks = new HashMap<>();
        this.keyBusy = new HashMap<>();
        s  = Server_KeystoreSrvProtocol
                .newSerializer();
        this.keyLocksGlobalLock = new ReentrantLock();


        Function<Map<Long,byte[]>,CompletableFuture<Void>> prepare = keys -> getLocks(new TreeSet<>(keys.keySet()));


        Consumer<Map<Long,byte[]>> commit = keys -> {
            synchronized (data) {
                data.putAll(keys);
            }
            releaseLocks(keys);
        };

        Consumer<Map<Long,byte[]>> abort = this::releaseLocksAbort;

        new Participant<>(id, ms, es, "KeyStoreSrv" + id, prepare, commit, abort);

        ms.registerHandler(Server_KeystoreSrvProtocol.GetControllerReq.class.getName(), this::get, es);

        System.out.println("Size: " + data.size());

        ms.start();
    }




    private void get(Address address, byte[] m) {
        System.out.println("GET in keystore");
        Server_KeystoreSrvProtocol.GetControllerReq prepReq = s.decode(m);
        Collection<Long> keys = prepReq.keys;
        Map<Long,byte[]> rp = new HashMap<>();
        for(Long e : keys){

            synchronized (data) {
                if (data.containsKey(e))
                    rp.put(e, data.get(e));
            }
        }
        Server_KeystoreSrvProtocol.GetControllerResp resp = new Server_KeystoreSrvProtocol.GetControllerResp(prepReq.txId,prepReq.pId,rp);
        ms.sendAsync(address,Server_KeystoreSrvProtocol.GetControllerResp.class.getName(),s.encode(resp));
    }


    private CompletableFuture<Void> getLocks(TreeSet<Long> keys) {
        this.keyLocksGlobalLock.lock();
        CompletableFuture [] readys = new CompletableFuture[keys.size()];
        int i=0;
        for(Long keyId: keys){
            if(keyBusy.containsKey(keyId) && keyBusy.get(keyId)){ //contem a chave e esta busy
                CompletableFuture<Void> cf = new CompletableFuture<>();
                cf.thenRun(()-> keyBusy.put(keyId,true));
                LinkedList<CompletableFuture<Void>> q = keyLocks.get(keyId);
                q.add(cf);
                keyLocks.put(keyId,q);
                readys[i]=cf;
            }
            else if (keyBusy.containsKey(keyId) && !keyBusy.get(keyId)){ //contem a chave e não está busy
                keyBusy.put(keyId,true);
                readys[i]=CompletableFuture.completedFuture(null);
            }
            else{ //nunca viu aquela chave na vida
                keyBusy.put(keyId,true);
                keyLocks.put(keyId, new LinkedList<>());
                readys[i]=CompletableFuture.completedFuture(null);
            }
            i++;
        }
        this.keyLocksGlobalLock.unlock();
        return CompletableFuture.allOf(readys);
    }


    private void releaseLocksAbort(Map<Long,byte[]> keys) {

        this.keyLocksGlobalLock.lock();
        for(Long keyId: keys.keySet()){

            LinkedList<CompletableFuture<Void>> q = keyLocks.get(keyId);

            if(q!=null) {
                if (q.isEmpty()) {
                    System.out.println("Unlocked " + keyId);
                    keyBusy.put(keyId, false);

                    boolean dataContainsKey;
                    synchronized (data) {
                        dataContainsKey = data.containsKey(keyId);
                    }
                    if (!dataContainsKey) {
                        keyLocks.remove(keyId);
                        keyBusy.remove(keyId);
                    }

                } else {
                    q.removeFirst().complete(null);
                }
            }
        }
        this.keyLocksGlobalLock.unlock();
    }

    private void releaseLocks(Map<Long,byte[]> keys) {
        this.keyLocksGlobalLock.lock();

        for(Long keyId: keys.keySet()){
            LinkedList<CompletableFuture<Void>> q = keyLocks.get(keyId);
            if(q!=null) {
                if (q.isEmpty()) {
                    System.out.println("Unlocked " + keyId);
                    keyBusy.put(keyId, false);
                } else {
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