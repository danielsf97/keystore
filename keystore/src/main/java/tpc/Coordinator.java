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
     * @param whenDone      Corresponde à ação a executar quando é conhecido o resultado da transação.
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

    /**
     * Função que realiza a recuperação do coordenador em caso de falha.
     */
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

    /**
     * Função responsável por dar inicío ao protocolo de 2PC. É criado um registo no Log
     * referente à transação e um registo no mapa das transações ativas. Invoca a função
     * que inicia a fase 1 do protocolo.
     *
     * @param clientTxId        Identificador da transação no contexto do cliente.
     * @param address           Endereço do Cliente.
     * @param separatedValues   Mapa dos diferentes dados da transação separados pelo participante a que se destina.
     */
    public void initProcess(int clientTxId, Address address, Map<Integer, T> separatedValues) {

        int txId = nextTxId.incrementAndGet();

        TwoPCTransaction tx = new TwoPCTransaction(txId, clientTxId, address);

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

    /**
     * Função responsável por dar inicío à fase 1 protocolo 2PC. Envia os diferentes
     * dados para cada participante e aguarda 15 segundos, que é o tempo máximo que um
     * participante tem para responder como se encontrando preparado para a transação.
     * Aborta caso contrário.
     *
     * @param txId              Identificador único da transação.
     * @param separatedValues   Mapa dos diferentes dados da transação separados pelo participante a que se destina.
     */
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

    /**
     * Função responsável por processar uma mensagem de resposta a um pedido de Prepared, recebida
     * de um determinado participante e referente a uma determinada transação. Verifica se os
     * participantes já se encontram todos preparados para a transação, iniciando a segunda fase do
     * protocolo 2PC em caso afirmativo.
     *
     * @param m         mensagem recebida no respetivo formato de serialização.
     */
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

    /**
     * Função responsável por dar inicío à fase 2 do protocolo 2PC. Envia pedidos
     * de commit aos participantes de forma iterativa até que a transação tenha
     * sido confirmada.
     *
     * @param e     Transação à qual se dará inicio a segunda fase do protocolo.
     */
    private void initTPC2(TwoPCTransaction e) {
        Collection<Integer> participants =  e.getParticipants();
        int txId = e.getId();

        for (Integer pId: participants) {
            TwoPCProtocol.ControllerCommitReq contReq = new TwoPCProtocol.ControllerCommitReq(txId, pId);
            String type = TwoPCProtocol.ControllerCommitReq.class.getName();

            sendIterative(pId, txId, contReq,type, Phase.COMMITTED);

        }
    }

    /**
     * Função responsável por processar uma mensagem de resposta a um pedido de Commit, recebida
     * de um determinado participante e referente a uma determinada transação. Verifica se a transação
     * existe e não for abortada. Caso todos os participantes já tenham confirmado a transação, a
     * transação é declarada como confirmada, estando pronta para a resposta ser devolvida.
     *
     * @param m         mensagem recebida no respetivo formato de serialização.
     */
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

    /**
     * Função responsável por dar inicío ao abort de uma transação. Envia pedidos
     * de abort aos participantes de forma iterativa até que a transação tenha
     * sido retrocedida.
     *
     * @param txId     Identificador único da transação.
     */
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

    /**
     * Função responsável realizar o devido processamento de receção de uma resposta
     * a um pedido de abort de transação. Dá a transação como retrocedida se todos
     * os participantes já tenham respondido ao pedido respetivo.
     *
     * @param bytes     Mensagem recebida serializada.
     */
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

    /**
     * Função que envia periodicamente (5 em 5 segundos) uma mensagem a um determinado
     * participante a fim de o colocar numa determinada fase no contexto da transação.
     *
     * @param pId           Identificador do participante.
     * @param txId          Identificador único da transação.
     * @param contReq       Pedido a ser realizado.
     * @param type          Tipo do pedido.
     * @param phase         Fase do participante que se espera obter com o pedido
     */
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
