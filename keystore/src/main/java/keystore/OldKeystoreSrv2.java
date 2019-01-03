package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import tpc.Log;
import tpc.Phase;
import tpc.TwoPCProtocol;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class OldKeystoreSrv  {


    private static final Address[] addresses = new Address[]{
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };

    private Scanner sc = new Scanner(System.in);
    private Map<Long, byte[]> data;
    private Map<Long, ReentrantLock> keyLocks;
    private Map<Integer,Map<Long, byte[]>> pendentPuts;
    private TreeMap<Integer,Map<Long, byte[]>> runningTransactions;

    private ReentrantLock runningTransactionsGlobalLock;
    private ReentrantLock keyLocksGlobalLock;

    private static ManagedMessagingService ms;
    private static Serializer s;
    private Log<Object> log;
    private int myId;
    private Address srv;

    private OldKeystoreSrv(int id){

        srv = Address.from("localhost", 12350);


        this.myId = id;

        ms = NettyMessagingService.builder()
                .withAddress(addresses[myId])
                .build();
        s  = Server_KeystoreSrvProtocol
                .newSerializer();

        ExecutorService es = Executors.newSingleThreadExecutor();

        this.log = new Log<>("KeyStoreSrv" + id);

        this.pendentPuts = new HashMap<>();
        this.data = new HashMap<>();
        this.keyLocks = new HashMap<>();
        this.runningTransactions = new TreeMap<>();

        this.runningTransactionsGlobalLock = new ReentrantLock();
        this.keyLocksGlobalLock = new ReentrantLock();


        es.execute(()->restore());

        //Recebe um pedido de prepared
        ms.registerHandler(TwoPCProtocol.ControllerPreparedReq.class.getName(), this::putTwoPC1, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommitReq.class.getName(), this::putTwoPC2, es);

        ms.registerHandler(Server_KeystoreSrvProtocol.GetControllerReq.class.getName(), this::get, es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortReq.class.getName(), this::abort, es);

        System.out.println("Size: " + data.size());

        ms.start();
    }

    private void restore() {
        HashMap<Integer,HashMap<Long,byte[]>> transKeys = new HashMap<>();
        HashMap<Integer,String> phases = new HashMap<>();


        List<Log.LogEntry> entries = log.read();
        entries.forEach((entry) -> {
            System.out.println(entry.toString());
            Object o = entry.getAction();
            if (o instanceof HashMap) {
                transKeys.put(entry.getTrans_id(),  (HashMap<Long, byte[]>) o);
            }
            else {
                phases.put(entry.getTrans_id(), (String) o );
            }
        });

        for(Map.Entry<Integer,String> entry : phases.entrySet()){
            if (entry.getValue().equals(Phase.PREPARED.toString())){
                HashMap<Long, byte[]> keys = transKeys.get(entry.getKey());
                pendentPuts.put(entry.getKey(),keys);
                for(Long key: keys.keySet()) {
                    ReentrantLock lock = new ReentrantLock();
                    lock.lock();
                    keyLocks.put(key, lock);
                }
            }
            else if (entry.getValue().equals(Phase.COMMITTED.toString())){
                HashMap<Long, byte[]> keys = transKeys.get(entry.getKey());
                data.putAll(keys);

            }
        }
    }

    private void abort(Address address, byte[] bytes) {
        System.out.println("ABORT");
        TwoPCProtocol.ControllerAbortReq ab = s.decode(bytes);
        int trans_id = ab.txId;


        if (runningTransactions.containsKey(trans_id)) {
            runningTransactions.remove(trans_id);

            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));

            System.out.println("Transaction " + trans_id + " before prepared!!");

        }
        else if(!pendentPuts.containsKey(trans_id)){
            //Quando a transação já fez rollback

            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));
            System.out.println("Transaction " + trans_id + " already doesn't exist!!");
        }
        else {
            // Quando conseguiu obter o lock(está prepared)
            // Quando ainda está à espera de obter o lock (não está nos pendentPuts)
            log.write(trans_id, Phase.ROLLBACKED.toString());

            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));

            synchronized (pendentPuts){
                Map<Long, byte[]> pendentKeys = pendentPuts.remove(trans_id);

                //Se em Prepared (Locks adquiridos)
                if(pendentKeys != null){
                    releaseLocksAbort(pendentKeys);

                }
            }

            System.out.println("Transaction " + trans_id + " rollbacked from prepared!!");
        }
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


    // Note: É possivel receber um pedido de prepared em algo que ele já fez abort????
    private void putTwoPC1(Address address, byte[] m){
        System.out.println("PUT in keystore");
        TwoPCProtocol.ControllerPreparedReq prepReq = s.decode(m);
        int trans_id = prepReq.txId;

        boolean pendentPutsContainsKey;
        synchronized (pendentPuts){
            pendentPutsContainsKey = pendentPuts.containsKey(trans_id);
        }

        if(pendentPutsContainsKey){ //se já está prepared
            System.out.println("Already sent prepared");
            TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
        }

        else { //se não está prepared

            Map<Long, byte[]> keys = (Map<Long, byte[]>)  prepReq.values;
            if( getLocks(new TreeSet<Long>(keys.keySet()))){
                synchronized (pendentPuts) {
                    pendentPuts.put(trans_id, keys);
                }
                log.write(trans_id,keys);
                log.write(trans_id, Phase.PREPARED.toString());
                TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId);
                ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
            }else{
                System.out.println("Posto à espera " + trans_id);
                this.runningTransactions.put(trans_id,keys);
            }

        }
    }

    private void putTwoPC2(Address address, byte[] m) {
        TwoPCProtocol.ControllerCommitReq commitReq = s.decode(m);
        int trans_id = commitReq.txId;

        Map<Long, byte[]> pendentKeys = null;
        synchronized (pendentPuts){
            pendentKeys = pendentPuts.remove(trans_id);
        }

        if (pendentKeys == null){ // se estiver committed é porque não existe nos pendentPuts
            System.out.println("OO");
            TwoPCProtocol.ControllerCommittedResp p = new TwoPCProtocol.ControllerCommittedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerCommittedResp.class.getName(),s.encode(p));
        }
        else {
            synchronized (data) {
                data.putAll(pendentKeys);
            }

            releaseLocks(pendentKeys);

            log.write(trans_id, Phase.COMMITTED.toString());
            System.out.println("Transaction " + trans_id + " commited!!");
            TwoPCProtocol.ControllerCommittedResp p = new TwoPCProtocol.ControllerCommittedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerCommittedResp.class.getName(),s.encode(p));
        }
    }

    private Boolean getLocks(TreeSet<Long> keys) {
        long threadId = Thread.currentThread().getId();
        System.out.println("Thread # " + threadId + " is doing this task");
        for(Long keyId: keys) {
            if (keyLocks.containsKey(keyId)) {
                if (keyLocks.get(keyId).isLocked()) {
                    System.out.println("Lock  nao Aquirido2");

                    return false;
                }
            }
        }
        for(Long keyId: keys){
            if(keyLocks.containsKey(keyId)){

                    keyLocks.get(keyId).lock();
                    System.out.println("Lock Aquirido2");
            }else{

                ReentrantLock lock = new ReentrantLock();
                lock.lock();
                System.out.println("Lock Aquirido1");
                keyLocks.put(keyId, lock);
            }
        }
        return true;
    }

    private void releaseLocksAbort(Map<Long,byte[]> keys) {

        for(Long keyId: keys.keySet()){
            keyLocks.get(keyId).unlock();

            boolean dataContainsKey;
            synchronized (data) {
                dataContainsKey = data.containsKey(keyId);
            }
            if(!dataContainsKey) keyLocks.remove(keyId);
        }

        List <Integer> toRemove = new ArrayList();

        for(Map.Entry<Integer, Map<Long, byte[]>> trans : runningTransactions.entrySet()){

            if(getLocks(new TreeSet<Long>(trans.getValue().keySet()))){
                int trans_id = trans.getKey();
                synchronized (pendentPuts) {
                    pendentPuts.put(trans_id, keys);
                }
                toRemove.add(trans_id);
                log.write(trans_id,keys);
                log.write(trans_id, Phase.PREPARED.toString());
                TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId);
                ms.sendAsync(srv, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
            }
        }

        toRemove.forEach((t)->{
            runningTransactions.remove(t);
        });
    }

    private void releaseLocks(Map<Long,byte[]> keys) {
        for(Long keyId: keys.keySet()){
            keyLocks.get(keyId).unlock();
        }
        long threadId = Thread.currentThread().getId();
        System.out.println("Thread release # " + threadId + " is doing this task");

        List <Integer> toRemove = new ArrayList();

        for(Map.Entry<Integer, Map<Long, byte[]>> trans : runningTransactions.entrySet()){

            if(getLocks(new TreeSet<Long>(trans.getValue().keySet()))){
                int trans_id = trans.getKey();
                synchronized (pendentPuts) {
                    pendentPuts.put(trans_id, keys);
                }
                toRemove.add(trans_id);
                log.write(trans_id,keys);
                log.write(trans_id, Phase.PREPARED.toString());
                TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId);
                ms.sendAsync(srv, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
            }
        }

        toRemove.forEach((t)->{
            runningTransactions.remove(t);
        });

    }

    public static void main(String[] args) {
        int id = Integer.parseInt(args[0]);
        new OldKeystoreSrv(id);
    }





}