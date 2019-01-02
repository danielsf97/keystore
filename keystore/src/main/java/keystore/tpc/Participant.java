package keystore.tpc;

import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import keystore.Serv;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class Participant <T> extends Serv {

    protected final Map<Long, T> data;
    private Map<Long, ReentrantLock> keyLocks;
    private final Map<Integer,Map<Long, T>> pendentPuts;
    private Set<Integer> runningTransactions;

    private static Serializer s ;

    private ReentrantLock runningTransactionsGlobalLock;
    private ReentrantLock keyLocksGlobalLock;

    private int myId;

    public Participant(Address address, String name, int id){
        super(address,name);

        this.myId = id;

        s = TwoPCProtocol.newSerializer();

        this.pendentPuts = new HashMap<>();
        this.data = new HashMap<>();
        this.keyLocks = new HashMap<>();
        this.runningTransactions = new TreeSet<>();

        this.runningTransactionsGlobalLock = new ReentrantLock();
        this.keyLocksGlobalLock = new ReentrantLock();

        ms.start();

        restore();

        ms.registerHandler(TwoPCProtocol.ControllerPreparedReq.class.getName(), this::put1, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommitReq.class.getName(), this::put2, es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortReq.class.getName(), this::abort, es);


    }


    private void restore() {
        HashMap<Integer, HashMap<Long,T>> transKeys = new HashMap<>();
        HashMap<Integer,String> phases = new HashMap<>();


        List<Log.LogEntry> entries = log.read();
        entries.forEach((entry) -> {
            System.out.println(entry.toString());
            Object o = entry.getAction();
            if (o instanceof HashMap) {
                transKeys.put(entry.getTrans_id(), (HashMap<Long, T>) o);
            }
            else {
                phases.put(entry.getTrans_id(), (String) o );
            }
        });


        for(Map.Entry<Integer,String> entry : phases.entrySet()){
            if (entry.getValue().equals(Phase.PREPARED.toString())){
                HashMap<Long, T> keys = transKeys.get(entry.getKey());
                pendentPuts.put(entry.getKey(),keys);
                runningTransactions.add(entry.getKey());
                for(Long key: keys.keySet()) {
                    ReentrantLock lock = new ReentrantLock();
                    lock.lock();
                    keyLocks.put(key, lock);
                }
            }
            else if (entry.getValue().equals(Phase.COMMITTED.toString())){
                HashMap<Long,T> keys = transKeys.get(entry.getKey());
                for(Map.Entry<Long, T> key: keys.entrySet()){
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

        boolean existentTransaction;

        runningTransactionsGlobalLock.lock();
        existentTransaction = runningTransactions.remove(trans_id);
        runningTransactionsGlobalLock.unlock();

        if(!existentTransaction){
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
                Map<Long,T> pendentKeys = pendentPuts.remove(trans_id);

                //Se em Prepared (Locks adquiridos)
                if(pendentKeys != null){
                    releaseLocksAbort(pendentKeys);

                }
            }

            System.out.println("Transaction " + trans_id + " rollbacked from prepared!!");
        }
    }


    private void put1(Address address, byte[] m){
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

            this.runningTransactionsGlobalLock.lock();
            this.runningTransactions.add(trans_id);
            this.runningTransactionsGlobalLock.unlock();

            Map<Long, T> keys = (Map<Long, T>) prepReq.values;
            getLocks(new TreeSet<>(keys.keySet()));

            this.runningTransactionsGlobalLock.lock();
            if (runningTransactions.contains(trans_id)){
                //se depois de obter os locks a transação ainda está ativa (not aborted)
                synchronized (pendentPuts) {
                    pendentPuts.put(trans_id, keys);
                }
                this.runningTransactionsGlobalLock.unlock();
                log.write(trans_id,keys);
                log.write(trans_id, Phase.PREPARED.toString());
                TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId);
                ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
            }else{
                //se ocorreu um abort
                this.runningTransactionsGlobalLock.unlock();
                releaseLocksAbort(keys);
            }

        }
    }

    private void put2(Address address, byte[] m) {
        TwoPCProtocol.ControllerCommitReq commitReq = s.decode(m);
        int trans_id = commitReq.txId;

        Map<Long, T> pendentKeys;
        synchronized (pendentPuts){
            pendentKeys = pendentPuts.remove(trans_id);
        }
        this.runningTransactionsGlobalLock.lock();
        this.runningTransactions.remove(trans_id);
        this.runningTransactionsGlobalLock.unlock();

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

    private void releaseLocksAbort(Map<Long,T> keys) {

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

    private void releaseLocks(Map<Long,T> keys) {
        for(Long keyId: keys.keySet()){
            keyLocks.get(keyId).unlock();
        }
    }
}
