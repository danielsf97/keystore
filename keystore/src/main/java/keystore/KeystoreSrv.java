package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class KeystoreSrv  {


    private static final Address[] addresses = new Address[]{
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };

    private Scanner sc = new Scanner(System.in);
    private Map<Long, byte[]> data;

    private Map<Long, ReentrantLock> keyLocks;
    ReentrantLock keyLocksGlobalLock;

    private static ManagedMessagingService ms;
    private static Serializer s;
    private Log<Object> log;
    private int myId;

    private Map<Integer,Map<Long, byte[]>> pendent_puts;

    private KeystoreSrv(int id){


        this.myId = id;

        ms = NettyMessagingService.builder()
                .withAddress(addresses[myId])
                .build();
        s  = TwoPCProtocol
                .newSerializer();

        ExecutorService es = Executors.newSingleThreadExecutor();

        this.log = new Log<>("KeyStoreSrv" + id);

        this.pendent_puts = new HashMap<>();

        data = new HashMap<>();
        keyLocks = new HashMap<>();
        keyLocksGlobalLock = new ReentrantLock();

        restore();

        //Recebe um pedido de prepared
        ms.registerHandler(TwoPCProtocol.ControllerPreparedReq.class.getName(), this::putTwoPC1, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommitReq.class.getName(), this::putTwoPC2, es);

        ms.registerHandler(TwoPCProtocol.GetControllerReq.class.getName(), this::get, es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortReq.class.getName(), this::abort, es);

        System.out.println("Size: " + data.size());

        ms.start();
    }

    private void restore() {
        HashMap<Integer,HashMap<Long,byte[]>> transKeys = new HashMap<>();
        HashMap<Integer,String> phases = new HashMap<>();
        int i = 0;
        boolean not_null = true;
        while(not_null){
            Log.LogEntry e = log.read(i);
            if (e==null) not_null = false;
            else {
                System.out.println(e.toString());
                Object o = e.getAction();
                if (o instanceof HashMap) {
                    transKeys.put(e.getTrans_id(), (HashMap<Long, byte[]>) o);
                }
                else {
                    phases.put(e.getTrans_id(), (String) o );
                }
            }
            i++;
        }
        for(Map.Entry<Integer,String> entry : phases.entrySet()){
            if (entry.getValue().equals(Phase.PREPARED.toString())){
                HashMap<Long, byte[]> keys = transKeys.get(entry.getKey());
                pendent_puts.put(entry.getKey(),keys);
                for(Long key: keys.keySet()) {
                    ReentrantLock lock = new ReentrantLock();
                    lock.lock();
                    keyLocks.put(key, lock);
                }
            }
            else if (entry.getValue().equals(Phase.COMMITED.toString())){
                HashMap<Long, byte[]> keys = transKeys.get(entry.getKey());
                for(Map.Entry<Long, byte[]> key: keys.entrySet()){
                    data.put(key.getKey(), key.getValue());
                    keyLocks.put(key.getKey(), new ReentrantLock());
                }

            }
        }
    }

    private void abort(Address address, byte[] bytes) {
        System.out.println("ABORT");
        TwoPCProtocol.ControllerAbortReq ab = s.decode(bytes);
        int trans_id = ab.txId;
        if(!pendent_puts.containsKey(trans_id)){
            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));
            System.out.println("Transaction " + trans_id + " already doesn't exist!!");
        }
        else {
            log.write(trans_id, Phase.ROLLBACKED.toString());
            releaseLocksAbort(pendent_puts.get(trans_id));
            pendent_puts.remove(trans_id);
            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));
            System.out.println("Transaction " + trans_id + " rollbacked from prepared!!");
        }
    }


    private void get(Address address, byte[] m) {
        System.out.println("PUT in keystore");
        TwoPCProtocol.GetControllerReq prepReq = s.decode(m);
        Collection<Long> keys = prepReq.keys;
        Map<Long,byte[]> rp = new HashMap<>();
        for(Long e : keys){
            if (data.containsKey(e))
                rp.put(e,data.get(e));
        }
        TwoPCProtocol.GetControllerResp resp = new TwoPCProtocol.GetControllerResp(prepReq.txId,prepReq.pId,rp);
        ms.sendAsync(address,TwoPCProtocol.GetControllerResp.class.getName(),s.encode(resp));
    }


    // Note: É possivel receber um pedido de prepared em algo que ele já fez abort????
    private void putTwoPC1(Address address, byte[] m){
        System.out.println("PUT in keystore");
        TwoPCProtocol.ControllerPreparedReq prepReq = s.decode(m);
        int trans_id = prepReq.txId;
        if(pendent_puts.containsKey(trans_id)){
            System.out.println("Already sent prepared");
            TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
        }
     /*   else if (log.actionAlreadyExists(trans_id, Phase.ROLLBACKED)){
            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));
        }*/
        else {
/*            System.out.print("You prepared for transaction " + trans_id + "?");
            String line = sc.nextLine();
            if (line != null && line.equals("yes")) {*/
                Map<Long, byte[]> keys =  prepReq.values;
                getLocks(new TreeSet<Long>(keys.keySet()));
                pendent_puts.put(trans_id, keys);
                log.write(trans_id,keys);
                log.write(trans_id, Phase.PREPARED.toString());
                TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId);
                ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
/*            } else {
                log.write(trans_id, Phase.ROLLBACKED.toString());
                TwoPCProtocol.ControllerAbortReq p = new TwoPCProtocol.ControllerAbortReq(trans_id,myId);
                ms.sendAsync(address, TwoPCProtocol.ControllerAbortReq.class.getName(),s.encode(p));
            }*/
        }
    }

    private void putTwoPC2(Address address, byte[] m) {
        TwoPCProtocol.ControllerCommitReq commitReq = s.decode(m);
        int trans_id = commitReq.txId;
        if (!pendent_puts.containsKey(trans_id)){ // se estiver committed é porque não existe nos pendent_puts
            System.out.println("OO");
            TwoPCProtocol.ControllerCommitedResp p = new TwoPCProtocol.ControllerCommitedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerCommitedResp.class.getName(),s.encode(p));
        }
        else {
            data.putAll(pendent_puts.get(trans_id));
            releaseLocks(pendent_puts.get(trans_id));
            pendent_puts.remove(trans_id);
            log.write(trans_id, Phase.COMMITED.toString());
            System.out.println("Transaction " + trans_id + " commited!!");
            TwoPCProtocol.ControllerCommitedResp p = new TwoPCProtocol.ControllerCommitedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerCommitedResp.class.getName(),s.encode(p));
        }
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
            }
        }
        this.keyLocksGlobalLock.unlock();
    }

    private void releaseLocksAbort(Map<Long,byte[]> keys) {
        this.keyLocksGlobalLock.lock();
        for(Long keyId: keys.keySet()){
            keyLocks.get(keyId).unlock();
            if(!data.containsKey(keyId)) keyLocks.remove(keyId);
        }
        this.keyLocksGlobalLock.unlock();
    }

    private void releaseLocks(Map<Long,byte[]> keys) {
        for(Long keyId: keys.keySet()){
            keyLocks.get(keyId).unlock();
        }
    }

    public static void main(String[] args) {
        int id = Integer.parseInt(args[0]);
        new KeystoreSrv(id);
    }





}