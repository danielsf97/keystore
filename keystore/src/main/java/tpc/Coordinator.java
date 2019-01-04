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


/**
 * Representa o coordenador do algoritmo de Two-Phase Commit.
 *
 * @param <T>
 */
public class Coordinator<T> {

    // ***************************************************************************
    // Variáveis
    // ***************************************************************************

    private final Address[] addresses;
    private final Log<Object> log;
    private static Serializer sp;
    private static AtomicInteger nextTxId;

    private HashMap<Integer, TwoPCTransaction> currentTransactions;
    private ReentrantLock currentTransactionsGlobalLock;
    private ManagedMessagingService ms;
    private BiConsumer<Boolean, TwoPCTransaction> whenDone;


    // ***************************************************************************
    // Construtores
    // ***************************************************************************


    /**
     * Construtor parametrizado de um coordenador.
     * @param addresses     Endereços dos participantes.
     * @param ms            Messaging service.
     * @param whenDone
     * @param name          Nome do log do coordenador
     * @param es            Executor service.
     */
    public Coordinator(Address[] addresses, ManagedMessagingService ms, BiConsumer<Boolean, TwoPCTransaction> whenDone, String name, ExecutorService es) {

        this.addresses = addresses;
        this.ms = ms;
        this.log = new Log<>(name);
        this.whenDone= whenDone;

        this.sp = TwoPCProtocol.newSerializer();

        this.currentTransactions = new HashMap<>();
        this.currentTransactionsGlobalLock = new ReentrantLock();
        this.nextTxId =  new AtomicInteger(0);

        restore();

        ms.registerHandler(TwoPCProtocol.ControllerPreparedResp.class.getName(), (o,m) -> {
            processTPC1(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommittedResp.class.getName(), (o, m) -> {
            processTPC2(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortResp.class.getName(), (o, m)-> {
            processAbortResp(m);
        }, es);
    }



    // ***************************************************************************
    // Restore
    // ***************************************************************************

    private void restore() {
        HashMap<Integer, SimpleTwoPCTransaction> keys = new HashMap<>();
        HashMap<Integer,String> phases = new HashMap<>();

        List<Log.LogEntry> entries = log.read();
        entries.forEach((entry) -> {
            Object action = entry.getAction();
            if (action instanceof SimpleTwoPCTransaction) {
                keys.put(entry.getId(), (SimpleTwoPCTransaction) action);
            }
            else {
                phases.put(entry.getId(), (String) action);
            }
        });


        for(Map.Entry<Integer,String> entry : phases.entrySet()) {
            if (entry.getValue().equals(Phase.STARTED.toString())) {
                SimpleTwoPCTransaction tx = keys.get(entry.getKey());
                TwoPCTransaction trans = new TwoPCTransaction(tx, Phase.STARTED);
                currentTransactions.put(trans.getId(), trans);
                initTPC1(trans.getId(),tx.participantsToT);
            }
            else if(entry.getValue().equals(Phase.PREPARED.toString())) {
                SimpleTwoPCTransaction tx = keys.get(entry.getKey());
                TwoPCTransaction trans = new TwoPCTransaction(tx, Phase.PREPARED);
                currentTransactions.put(trans.getId(), trans);
                initTPC2(trans);
            }
            else if(entry.getValue().equals(Phase.ABORT.toString())) {
                SimpleTwoPCTransaction tx = keys.get(entry.getKey());
                TwoPCTransaction trans = new TwoPCTransaction(tx, Phase.PREPARED);
                currentTransactions.put(trans.getId(), trans);
                initAbort(trans.getId());
            }
        }

        nextTxId.addAndGet(phases.size());
    }


    // ***************************************************************************
    // Two-Phase Commit - Phase 1
    // ***************************************************************************

    public void initProcess(int clientTxId, Address c, Map<Integer, T> separatedValues) {

        int txId = nextTxId.incrementAndGet();

        TwoPCTransaction tx = new TwoPCTransaction(txId, clientTxId, c);

        this.currentTransactionsGlobalLock.lock();
        this.currentTransactions.put(txId, tx);
        this.currentTransactionsGlobalLock.unlock();

        tx.setParticipants(separatedValues.keySet());
        SimpleTwoPCTransaction<T> simpleTx = new SimpleTwoPCTransaction<>(tx.getId(), tx.getClientTxId(), tx.getAddress(), separatedValues);

        synchronized (log) {
            log.write(tx.getId(), simpleTx);
            log.write(tx.getId(), Phase.STARTED.toString());
        }

        initTPC1(tx.getId(), separatedValues);
    }
    
    
    private void initTPC1(int txId, Map<Integer, T> separatedValues) {

        for(Map.Entry<Integer, T> ksValues : separatedValues.entrySet()) {
            int pId = ksValues.getKey();
            TwoPCProtocol.ControllerPreparedReq<T> contReq = new TwoPCProtocol.ControllerPreparedReq<>(txId, pId, ksValues.getValue());
            String type = TwoPCProtocol.ControllerPreparedReq.class.getName();
            ms.sendAsync(addresses[pId], type, sp.encode(contReq));
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(() -> {
            currentTransactionsGlobalLock.lock();
            TwoPCTransaction e = currentTransactions.get(txId);
            currentTransactionsGlobalLock.unlock();

            e.lock();

            if (!e.checkParticipantsPhases(Phase.PREPARED)) {
                e.unlock();

                initAbort(txId);
            }
            else e.unlock();
        }, 15, TimeUnit.SECONDS);
    }


    private void processTPC1(byte[] m) {
        TwoPCProtocol.ControllerPreparedResp rp = sp.decode(m);

        this.currentTransactionsGlobalLock.lock();
        TwoPCTransaction e = currentTransactions.get(rp.txId);
        this.currentTransactionsGlobalLock.unlock();

        e.lock();

        if (e.getPhase() != Phase.ABORT) {
            e.setParticipantStatus(rp.pId, Phase.PREPARED);
            if (e.checkParticipantsPhases(Phase.PREPARED)) {
                e.setPhase(Phase.PREPARED);
                whenDone.accept(true, e);
                e.unlock();

                synchronized (log) {
                    log.write(rp.txId, Phase.PREPARED.toString());
                }

                initTPC2(e);
            }
            else e.unlock();
        }
        else e.unlock();
    }


    // ***************************************************************************
    // Two-Phase Commit - Phase 2
    // ***************************************************************************

    private void initTPC2(TwoPCTransaction e) {
        Collection<Integer> participants =  e.getParticipants();
        int txId = e.getId();

        for (Integer pId: participants) {
            TwoPCProtocol.ControllerCommitReq contReq = new TwoPCProtocol.ControllerCommitReq(txId, pId);
            String type = TwoPCProtocol.ControllerCommitReq.class.getName();

            sendIterative(pId, txId, contReq,type, Phase.COMMITTED);

        }
    }


    private void processTPC2(byte[] m) {
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
                    this.currentTransactions.remove(rp.txId);
                    this.currentTransactionsGlobalLock.unlock();

                    synchronized (log) {
                        log.write(rp.txId, Phase.COMMITTED.toString());
                    }

                } else e.unlock();
            } else e.unlock();
        }
        else this.currentTransactionsGlobalLock.unlock();
    }


    // ***************************************************************************
    // Abort
    // ***************************************************************************

    private void initAbort(int txId) {

        currentTransactionsGlobalLock.lock();
        TwoPCTransaction e = currentTransactions.get(txId);
        currentTransactionsGlobalLock.unlock();

        e.lock();

        if(e.getPhase() != Phase.ROLLBACKED && e.getPhase() != Phase.ABORT) {

            e.setPhase(Phase.ABORT);
            whenDone.accept(false, e);
            e.unlock();

            synchronized (log) {
                log.write(txId, Phase.ABORT.toString());
            }

            String type = TwoPCProtocol.ControllerAbortReq.class.getName();

            for (Integer pId : e.getParticipants()) {
                TwoPCProtocol.ControllerAbortReq abort = new TwoPCProtocol.ControllerAbortReq(txId, pId);

                sendIterative(pId, txId, abort, type, Phase.ROLLBACKED);
            }
        }
        else e.unlock();
    }


    private void processAbortResp(byte[] bytes) {
        TwoPCProtocol.ControllerAbortResp rp = sp.decode(bytes);

        this.currentTransactionsGlobalLock.lock();
        if(currentTransactions.containsKey(rp.txId)) {

            TwoPCTransaction e = currentTransactions.get(rp.txId);
            this.currentTransactionsGlobalLock.unlock();

            e.lock();

            if (e.getPhase() != Phase.ROLLBACKED) {
                e.setParticipantStatus(rp.pId, Phase.ROLLBACKED);
                if (e.checkParticipantsPhases(Phase.ROLLBACKED)) {
                    e.setPhase(Phase.ROLLBACKED);
                    e.unlock();

                    this.currentTransactionsGlobalLock.lock();
                    currentTransactions.remove(rp.txId);
                    this.currentTransactionsGlobalLock.unlock();

                    synchronized (log) {
                        log.write(rp.txId, Phase.ROLLBACKED.toString());
                    }

                } else e.unlock();

            } else e.unlock();

        } else this.currentTransactionsGlobalLock.unlock();
    }


    // ***************************************************************************
    // Send functions
    // ***************************************************************************

    private void sendIterative(int pId, int txId, TwoPCProtocol.ControllerReq contReq, String type, Phase phase) {
        ms.sendAsync(addresses[pId], type, sp.encode(contReq));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {

            this.currentTransactionsGlobalLock.lock();
            TwoPCTransaction e = this.currentTransactions.get(txId);
            this.currentTransactionsGlobalLock.unlock();

            e.lock();

            if (e.getParticipantStatus(pId) == phase) {
                e.unlock();
                scheduler.shutdown();
            }
            else {
                e.unlock();
                this.ms.sendAsync(addresses[pId], type, sp.encode(contReq));
            }

        }, 5, 5, TimeUnit.SECONDS);
    }
}
