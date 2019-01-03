package tpc;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class Participant<T> {


    private final Map<Integer,T> pendentTransactions;
    private Set<Integer> runningTransactions;

    private static Serializer s ;

    private ReentrantLock runningTransactionsGlobalLock;

    private int myId;
    private Log<Object> log;
    private ManagedMessagingService ms;
    private Function<T, CompletableFuture<Void>> prepare;
    private Consumer<T> commit;
    private Consumer<T> abort;

    public Participant(int id, ManagedMessagingService ms, ExecutorService es, String name, Function<T,CompletableFuture<Void>> prepare, Consumer<T> commit, Consumer<T> abort) {

        this.myId = id;

        this.ms = ms;
        this.log = new Log<>(name);
        this.prepare = prepare;
        this.commit = commit;
        this.abort = abort;
        s = TwoPCProtocol.newSerializer();

        this.pendentTransactions = new HashMap<>();

        this.runningTransactions = new TreeSet<>();

        this.runningTransactionsGlobalLock = new ReentrantLock();


        restore();

        ms.registerHandler(TwoPCProtocol.ControllerPreparedReq.class.getName(), this::phase1, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommitReq.class.getName(), this::phase2, es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortReq.class.getName(), this::abort, es);


    }



    private void restore() {
        HashMap<Integer, T> transKeys = new HashMap<>();
        HashMap<Integer,String> phases = new HashMap<>();


        List<Log.LogEntry> entries = log.read();
        entries.forEach((entry) -> {
            System.out.println(entry.toString());
            Object o = entry.getAction();
            if (o instanceof HashMap) {
                transKeys.put(entry.getTrans_id(), (T) o);
            }
            else {
                phases.put(entry.getTrans_id(), (String) o );
            }
        });


        for(Map.Entry<Integer,String> entry : phases.entrySet()){
            if (entry.getValue().equals(Phase.PREPARED.toString())){
                T keys = transKeys.get(entry.getKey());
                pendentTransactions.put(entry.getKey(),keys);
                runningTransactions.add(entry.getKey());
                prepare.apply(keys);
            }
            else if (entry.getValue().equals(Phase.COMMITTED.toString())){
                T keys = transKeys.get(entry.getKey());
                commit.accept(keys);

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
            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));
            System.out.println("Transaction " + trans_id + " already doesn't exist!!");
        }
        else {
            // Quando conseguiu obter o lock(está prepared)
            // Quando ainda está à espera de obter o lock (não está nos pendentTransactions)
            log.write(trans_id, Phase.ROLLBACKED.toString());

            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));

            synchronized (pendentTransactions){
                T pendentKeys = pendentTransactions.remove(trans_id);

                //Se em Prepared (Locks adquiridos)
                if(pendentKeys != null){
                    abort.accept(pendentKeys);
                }
            }

            System.out.println("Transaction " + trans_id + " rollbacked from prepared!!");
        }
    }


    private void phase1(Address address, byte[] m){
        System.out.println("PUT in keystore");
        TwoPCProtocol.ControllerPreparedReq prepReq = s.decode(m);
        int trans_id = prepReq.txId;
        boolean pendentPutsContainsKey;
        synchronized (pendentTransactions){
            pendentPutsContainsKey = pendentTransactions.containsKey(trans_id);
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

            T values = (T) prepReq.values;
            prepare.apply(values).thenRun(()->{
                System.out.println("Lock adquirido para tx:" + trans_id);
                this.runningTransactionsGlobalLock.lock();
                if (runningTransactions.contains(trans_id)) {
                    synchronized (pendentTransactions) {
                        pendentTransactions.put(trans_id, values);
                    }
                    this.runningTransactionsGlobalLock.unlock();
                    log.write(trans_id, values);
                    log.write(trans_id, Phase.PREPARED.toString());
                    TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id, myId);
                    ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(), s.encode(p));
                }
                else {
                    this.runningTransactionsGlobalLock.unlock();
                    abort.accept(values);
                }
            });


/*
            this.runningTransactionsGlobalLock.lock();
            if (runningTransactions.contains(trans_id)){
                //se depois de obter os locks a transação ainda está ativa (not aborted)
                synchronized (pendentTransactions) {
                    pendentTransactions.put(trans_id, values);
                }
                this.runningTransactionsGlobalLock.unlock();
                log.write(trans_id,values);
                log.write(trans_id, Phase.PREPARED.toString());
                TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId);
                ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
            }else{
                //se ocorreu um abort
                this.runningTransactionsGlobalLock.unlock();
                abort.accept(values);
            }*/

        }
    }

    private void phase2(Address address, byte[] m) {
        TwoPCProtocol.ControllerCommitReq commitReq = s.decode(m);
        int trans_id = commitReq.txId;

        T pendentKeys;
        synchronized (pendentTransactions){
            pendentKeys = pendentTransactions.remove(trans_id);
        }
        this.runningTransactionsGlobalLock.lock();
        this.runningTransactions.remove(trans_id);
        this.runningTransactionsGlobalLock.unlock();

        if (pendentKeys == null){ // se estiver committed é porque não existe nos pendentTransactions
            System.out.println("OO");
            TwoPCProtocol.ControllerCommittedResp p = new TwoPCProtocol.ControllerCommittedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerCommittedResp.class.getName(),s.encode(p));
        }
        else {

            commit.accept(pendentKeys);

            log.write(trans_id, Phase.COMMITTED.toString());
            System.out.println("Transaction " + trans_id + " commited!!");
            TwoPCProtocol.ControllerCommittedResp p = new TwoPCProtocol.ControllerCommittedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerCommittedResp.class.getName(),s.encode(p));
        }
    }

}
