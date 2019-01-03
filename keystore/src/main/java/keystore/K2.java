package keystore;

import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import tpc.Participant;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class KeystoreSrv extends Serv {


    private static final Address[] addresses = new Address[]{
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };

    private final Map<Long, byte[]> data;
    private Map<Long, ReentrantLock> keyLocks;
    private ReentrantLock keyLocksGlobalLock;

    private static Serializer s;


    private KeystoreSrv(int id){
        super(addresses[id]);

        this.data = new HashMap<>();
        this.keyLocks = new HashMap<>();

        s  = Server_KeystoreSrvProtocol
                .newSerializer();
        this.keyLocksGlobalLock = new ReentrantLock();


        Consumer<Map<Long,byte[]>> prepare = keys -> getLocks(new TreeSet<>(keys.keySet()));


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




    public static void main(String[] args) {
        int id = Integer.parseInt(args[0]);
        new KeystoreSrv(id);
    }



    private void getLocks(TreeSet<Long> keys) {
        this.keyLocksGlobalLock.lock();
        for(Long keyId: keys){
            if(keyLocks.containsKey(keyId)){
                //fazer unlock ao global porque podemos bloquear no lock da chave
                this.keyLocksGlobalLock.unlock();
                keyLocks.get(keyId).lock();
                this.keyLocksGlobalLock.lock();
            }else{
                ReentrantLock lock = new ReentrantLock();
                lock.lock();
                keyLocks.put(keyId, lock);
                System.out.println("Locked " + keyId);
            }
        }
        this.keyLocksGlobalLock.unlock();
    }

    private void releaseLocksAbort(Map<Long,byte[]> keys) {

        this.keyLocksGlobalLock.lock();
        for(Long keyId: keys.keySet()){
            keyLocks.get(keyId).unlock();

            boolean dataContainsKey;
            synchronized (data) {
                dataContainsKey = data.containsKey(keyId);
            }
            if(!dataContainsKey) keyLocks.remove(keyId);
        }
        this.keyLocksGlobalLock.unlock();
    }


    private void releaseLocks(Map<Long,byte[]> keys) {
        this.keyLocksGlobalLock.lock();

        for(Long keyId: keys.keySet()){
            if (keyLocks.get(keyId).isLocked()) {
                System.out.println("Unlocked " + keyId);
                keyLocks.get(keyId).unlock();
           }
        }
        this.keyLocksGlobalLock.unlock();

    }




}