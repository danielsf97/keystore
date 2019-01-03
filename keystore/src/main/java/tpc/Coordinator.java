package tpc;


import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

public class Coordinator<T> {

    private final Address[] addresses;

    private static Serializer sp ;

    private HashMap<Integer, TwoPCTransaction> currentTransactions;
    private ReentrantLock currentTransactionsGlobalLock;

    private ManagedMessagingService ms;

    private static AtomicInteger nextTxId;

    private BiConsumer<Boolean,TwoPCTransaction> whenDone;
    private Log<Object> log;


    public Coordinator(Address[] addresses, ManagedMessagingService ms, BiConsumer<Boolean,TwoPCTransaction> whenDone, String name, ExecutorService es) {

        this.addresses = addresses;
        this.ms = ms;
        this.log = new Log<>(name);
        this.whenDone= whenDone;

        sp = TwoPCProtocol.newSerializer();

        this.currentTransactions = new HashMap<>();
        this.currentTransactionsGlobalLock = new ReentrantLock();
        nextTxId =  new AtomicInteger(0);

        System.out.println("Size: " + currentTransactions.size());

        restore();

        ms.registerHandler(TwoPCProtocol.ControllerPreparedResp.class.getName(), (o,m) -> {
            System.out.println("Response from keystoresrv");
            processTPC1(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommittedResp.class.getName(), (o, m) -> {
            processTPC2(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortResp.class.getName(),(o,m)->{
            processAbortResp(m);
        }, es);


    }



    /////////////////////////Restore///////////////////////////

    private void restore() {
        HashMap<Integer, SimpleTwoPCTransaction> keys = new HashMap<>();
        HashMap<Integer,String> phases = new HashMap<>();

        List<Log.LogEntry> entries = log.read();
        entries.forEach((entry) -> {
            System.out.println(entry.toString());
            Object o = entry.getAction();
            if (o instanceof SimpleTwoPCTransaction) {
                keys.put(entry.getTrans_id(), (SimpleTwoPCTransaction) o);
            }
            else {
                phases.put(entry.getTrans_id(), (String) o );
            }
        });


        for(Map.Entry<Integer,String> entry : phases.entrySet()){
            if (entry.getValue().equals(Phase.STARTED.toString())){
                SimpleTwoPCTransaction tx = keys.get(entry.getKey());
                TwoPCTransaction trans = new TwoPCTransaction(tx, Phase.STARTED);
                currentTransactions.put(trans.getId(), trans);
                initTPC1(trans.getId(),tx.participantsToT);
            }
            else if (entry.getValue().equals(Phase.PREPARED.toString())){
                SimpleTwoPCTransaction tx = keys.get(entry.getKey());
                TwoPCTransaction trans = new TwoPCTransaction(tx, Phase.PREPARED);
                currentTransactions.put(trans.getId(), trans);
                initTPC2(trans);
            }
            else if (entry.getValue().equals(Phase.ABORT.toString())){
                SimpleTwoPCTransaction tx = keys.get(entry.getKey());
                TwoPCTransaction trans = new TwoPCTransaction(tx, Phase.PREPARED);
                currentTransactions.put(trans.getId(), trans);
                initAbort(trans.getId());
            }
        }

        nextTxId.addAndGet(phases.size());
        System.out.println("IIIIIIIIIIIIIIIIIII: " + nextTxId.get());
    }


    /////////////////////////Phase1///////////////////////////

    public void initProcess(int client_txId, Address c, Map<Integer,T> separatedValues) {

        int txId = nextTxId.incrementAndGet();

        TwoPCTransaction tx = new TwoPCTransaction(txId,client_txId,c);
        this.currentTransactionsGlobalLock.lock();
        currentTransactions.put(txId, tx);
        this.currentTransactionsGlobalLock.unlock();

        tx.setParticipants(separatedValues.keySet());
        SimpleTwoPCTransaction<T> simpleTx = new SimpleTwoPCTransaction<>(tx.getId(),tx.get_client_txId(),tx.getAddress(),separatedValues);
        log.write(tx.getId(),simpleTx);
        log.write(tx.getId(),Phase.STARTED.toString());
        initTPC1(tx.getId(), separatedValues);
    }
    
    
    private void initTPC1(int txId, Map<Integer, T> separatedValues) {

        for (Map.Entry<Integer, T> ksValues : separatedValues.entrySet()) {
            int pId = ksValues.getKey();
            TwoPCProtocol.ControllerPreparedReq<T> contReq = new TwoPCProtocol.ControllerPreparedReq<>(txId, pId, ksValues.getValue());
            String type = TwoPCProtocol.ControllerPreparedReq.class.getName();
            send_iteractive(pId,txId,contReq,type);

        }
    }


    private void processTPC1(byte[] m) {
        TwoPCProtocol.ControllerPreparedResp rp = sp.decode(m);

        this.currentTransactionsGlobalLock.lock();
        TwoPCTransaction e = currentTransactions.get(rp.txId);
        this.currentTransactionsGlobalLock.unlock();

        e.lock();
        if (e.getPhase() != Phase.ABORT){
            System.out.println("Init Commit: " + rp.pId);
            e.setParticipantStatus(rp.pId, Phase.PREPARED);
            if (e.checkParticipantsPhases(Phase.PREPARED)){
                e.setPhase(Phase.PREPARED);
                whenDone.accept(true,e);
                e.unlock();

                log.write(rp.txId,Phase.PREPARED.toString());


                System.out.println("Init Commit");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                initTPC2(e);
            }else e.unlock();
        }else e.unlock();
    }



    /////////////////////////Phase2///////////////////////////


    private void initTPC2(TwoPCTransaction e) {
        Collection<Integer> participants =  e.getParticipants();
        int txId = e.getId();
        for (Integer pId: participants){
            TwoPCProtocol.ControllerCommitReq contReq = new TwoPCProtocol.ControllerCommitReq(txId, pId);
            String type = TwoPCProtocol.ControllerCommitReq.class.getName();

            send_iteractive2(pId,txId,contReq,type, Phase.COMMITTED);

        }
    }


    private void processTPC2(byte[] m){
        System.out.println("A acabar");
        TwoPCProtocol.ControllerCommittedResp rp = sp.decode(m);

        this.currentTransactionsGlobalLock.lock();
        if (currentTransactions.containsKey(rp.txId)) {

            TwoPCTransaction e = currentTransactions.get(rp.txId);
            this.currentTransactionsGlobalLock.unlock();

            e.lock();
            if (e.getPhase() != Phase.ABORT) {
                e.setParticipantStatus(rp.pId, Phase.COMMITTED);
                if (e.checkParticipantsPhases(Phase.COMMITTED)) {
                    e.setPhase(Phase.COMMITTED);
                    e.unlock();

                    this.currentTransactionsGlobalLock.lock();
                    currentTransactions.remove(rp.txId);
                    this.currentTransactionsGlobalLock.unlock();

                    log.write(rp.txId, Phase.COMMITTED.toString());
                    System.out.println("Done");
                }else e.unlock();
            }else e.unlock();

        }else{
            this.currentTransactionsGlobalLock.unlock();
        }
    }


    /////////////////////////Abort////////////////////////////


    private void initAbort(int txId){

        currentTransactionsGlobalLock.lock();
        TwoPCTransaction e = currentTransactions.get(txId);
        currentTransactionsGlobalLock.unlock();

        e.lock();
        if (e.getPhase() != Phase.ROLLBACKED && e.getPhase() != Phase.ABORT) {

            e.setPhase(Phase.ABORT);
            whenDone.accept(false,e);
            e.unlock();

            log.write(txId, Phase.ABORT.toString());




            String type = TwoPCProtocol.ControllerAbortReq.class.getName();
            for (Integer pId : e.getParticipants()) {
                TwoPCProtocol.ControllerAbortReq abortt = new TwoPCProtocol.ControllerAbortReq(txId, pId);
                ms.sendAsync(addresses[pId], type, sp.encode(abortt));
                send_iteractive2(pId, txId, abortt, type, Phase.ROLLBACKED);
            }
        }else{
            e.unlock();
        }
    }


    private void processAbortResp(byte[] bytes) {
        TwoPCProtocol.ControllerAbortResp rp = sp.decode(bytes);

        this.currentTransactionsGlobalLock.lock();
        if (currentTransactions.containsKey(rp.txId)) {

            TwoPCTransaction e = currentTransactions.get(rp.txId);
            this.currentTransactionsGlobalLock.unlock();

            e.lock();

            if (e.getPhase() != Phase.ROLLBACKED) {
                System.out.println("FASE1:" + e.getParticipantStatus(rp.pId).toString());
                e.setParticipantStatus(rp.pId, Phase.ROLLBACKED);
                System.out.println("FASE2:" + e.getParticipantStatus(rp.pId).toString());
                if (e.checkParticipantsPhases(Phase.ROLLBACKED)) {
                    e.setPhase(Phase.ROLLBACKED);
                    e.unlock();

                    this.currentTransactionsGlobalLock.lock();
                    currentTransactions.remove(rp.txId);
                    this.currentTransactionsGlobalLock.unlock();
                    System.out.println("All prepared for tx:" + e.getId());

                    log.write(rp.txId, Phase.ROLLBACKED.toString());

                    System.out.println("ABOOOOOOOOOOORRRTTTTTTTTTTTEEDDD");
                }else e.unlock();
            }else e.unlock();

        }else{
            this.currentTransactionsGlobalLock.unlock();
        }
    }


    /////////////////////////SENDS////////////////////////////


    private void send_iteractive(int pId, int txId, TwoPCProtocol.ControllerReq contReq, String type){
        ms.sendAsync(addresses[pId],
                type,
                sp.encode(contReq));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(()->{
            System.out.println("CHECKING");
            currentTransactionsGlobalLock.lock();
            TwoPCTransaction e = currentTransactions.get(txId);
            currentTransactionsGlobalLock.unlock();

            e.lock();
            if (e.getParticipantStatus(pId) == Phase.STARTED) {
                System.out.println("CHECKING ABORT");
                e.unlock();

                initAbort(txId);
            }
            else e.unlock();
        }, 15, TimeUnit.SECONDS);
    }


    private void send_iteractive2(int pId, int txId, TwoPCProtocol.ControllerReq contReq, String type, Phase phase){
        ms.sendAsync(addresses[pId],
                type,
                sp.encode(contReq));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(()->{
            System.out.println("CHECKING2 " + phase.toString());

            currentTransactionsGlobalLock.lock();
            TwoPCTransaction e = currentTransactions.get(txId);
            currentTransactionsGlobalLock.unlock();

            e.lock();
            if (e.getParticipantStatus(pId) == phase){
                e.unlock();
                scheduler.shutdown();
            }
            else{
                e.unlock();
                ms.sendAsync(addresses[pId],
                        type,
                        sp.encode(contReq));
            }


        }, 5, 5, TimeUnit.SECONDS);
    }


}
