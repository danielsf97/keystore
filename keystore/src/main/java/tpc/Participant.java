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

/**
 * Representa um participante do algoritmo de Two-Phase Commit.
 *
 * @param <T>
 */
public class Participant<T> {

    // ***************************************************************************
    // Variáveis
    // ***************************************************************************
    
    private final Map<Integer, T> pendentTransactions;
    private Set<Integer> runningTransactions;

    private static Serializer s;

    private ReentrantLock runningTransactionsGlobalLock;

    private int myId;
    private Log<Object> log;
    private ManagedMessagingService ms;
    private Function<T, CompletableFuture<Void>> prepare;
    private Consumer<T> commit;
    private Consumer<T> abort;


    // ***************************************************************************
    // Construtores
    // ***************************************************************************

    /**
     * Construtor parametrizado de um participante.
     * @param id            Endereços dos participantes.
     * @param ms            Messaging service.
     * @param name          Nome do log do coordenador.
     * @param prepare       Corresponde ao que é necessário fazer para o participante
     *                          se encontrar preparado para a transação.
     * @param commit        Corresponde ao que efetuar no momento do commit.
     * @param abort         Corresponde ao que efetuar em caso de abort da transação.
     */
    public Participant(int id, ManagedMessagingService ms, ExecutorService es, String name, Function<T,CompletableFuture<Void>> prepare, Consumer<T> commit, Consumer<T> abort) {
        this.myId = id;
        this.ms = ms;
        this.log = new Log<>(name);
        this.prepare = prepare;
        this.commit = commit;
        this.abort = abort;
        this.pendentTransactions = new HashMap<>();
        this.runningTransactions = new TreeSet<>();
        this.runningTransactionsGlobalLock = new ReentrantLock();

        s = TwoPCProtocol.newSerializer();

        restore();

        ms.registerHandler(TwoPCProtocol.ControllerPreparedReq.class.getName(), this::phase1, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommitReq.class.getName(), this::phase2, es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortReq.class.getName(), this::abort, es);
    }


    // ***************************************************************************
    // Restore
    // ***************************************************************************

    /**
     * Função que realiza a recuperação de um participante em caso de falha.
     */
    private void restore() {
        
        HashMap<Integer, T> transKeys = new HashMap<>();
        HashMap<Integer,String> phases = new HashMap<>();


        List<Log.LogEntry> entries = log.read();
        entries.forEach((entry) -> {

            Object o = entry.getAction();

            if(o instanceof HashMap) {
                transKeys.put(entry.getId(), (T) o);
            }
            else {
                phases.put(entry.getId(), (String) o );
            }
        });


        for(Map.Entry<Integer,String> entry : phases.entrySet()) {
            if (entry.getValue().equals(Phase.PREPARED.toString())) {
                T keys = transKeys.get(entry.getKey());
                pendentTransactions.put(entry.getKey(),keys);
                runningTransactions.add(entry.getKey());
                prepare.apply(keys);
            }
            else if (entry.getValue().equals(Phase.COMMITTED.toString())) {
                T keys = transKeys.get(entry.getKey());
                commit.accept(keys);
            }
        }
    }


    // ***************************************************************************
    // Abort
    // ***************************************************************************

    /**
     * Função responsável por processar um pedido de abort da transação vindo do servidor.
     *
     * @param address      Endereço do servidor.
     * @param bytes        Mensagem de pedido de abort no formato serializado.
     */
    private void abort(Address address, byte[] bytes) {
        TwoPCProtocol.ControllerAbortReq ab = s.decode(bytes);
        int transId = ab.txId;

        boolean existentTransaction;

        this.runningTransactionsGlobalLock.lock();
        existentTransaction = this.runningTransactions.remove(transId);
        this.runningTransactionsGlobalLock.unlock();

        if(!existentTransaction) {
            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(transId, myId);
            this.ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(), s.encode(p));
        }
        else {
            this.log.write(transId, Phase.ROLLBACKED.toString());

            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(transId, myId);
            this.ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(), s.encode(p));

            synchronized (this.pendentTransactions) {
                T pendentKeys = this.pendentTransactions.remove(transId);

                // Se em prepared (Locks adquiridos)
                if(pendentKeys != null) {
                    this.abort.accept(pendentKeys);
                }
            }
        }
    }


    // ***************************************************************************
    // Two-Phase Commit - Phase 1
    // ***************************************************************************

    /**
     * Função responsável por processar uma mensagem do tipo Prepared vinda do servidor.
     * Na maioria dos casos dando início à primeira fase do protocolo 2PC.
     *
     * @param address      Endereço do servidor.
     * @param m            Mensagem correspondente ao pedido de Prepared por parte do servidor.
     */
    private void phase1(Address address, byte[] m) {
        TwoPCProtocol.ControllerPreparedReq prepReq = s.decode(m);
        int transId = prepReq.txId;

        boolean pendentPutsContainsKey;
        synchronized (pendentTransactions) {
            pendentPutsContainsKey = pendentTransactions.containsKey(transId);
        }

        if(pendentPutsContainsKey) { //se já está prepared
            TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(transId, myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(), s.encode(p));
        }

        else { //se não está prepared

            this.runningTransactionsGlobalLock.lock();
            this.runningTransactions.add(transId);
            this.runningTransactionsGlobalLock.unlock();

            T values = (T) prepReq.values;
            prepare.apply(values).thenRun(() -> {
                this.runningTransactionsGlobalLock.lock();

                if (runningTransactions.contains(transId)) {
                    synchronized (pendentTransactions) {
                        pendentTransactions.put(transId, values);
                    }

                    this.runningTransactionsGlobalLock.unlock();
                    this.log.write(transId, values);
                    this.log.write(transId, Phase.PREPARED.toString());
                    TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(transId, myId);
                    this.ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(), s.encode(p));
                }
                else {
                    this.runningTransactionsGlobalLock.unlock();
                    abort.accept(values);
                }
            });
        }
    }


    // ***************************************************************************
    // Two-Phase Commit - Phase 2
    // ***************************************************************************

    /**
     * Função responsável por processar uma mensagem do tipo Commit vinda do servidor.
     * Na maioria dos casos dando início à segunda fase do protocolo 2PC, ou seja à fase de commit.
     *
     * @param address      Endereço do servidor.
     * @param m            Mensagem correspondente ao pedido de Commit por parte do servidor.
     */
    private void phase2(Address address, byte[] m) {
        TwoPCProtocol.ControllerCommitReq commitReq = s.decode(m);
        int transId = commitReq.txId;

        T pendentKeys;
        
        synchronized(pendentTransactions) {
            pendentKeys = pendentTransactions.remove(transId);
        }
        
        this.runningTransactionsGlobalLock.lock();
        this.runningTransactions.remove(transId);
        this.runningTransactionsGlobalLock.unlock();

        if(pendentKeys == null) { // se estiver committed é porque não existe nas pendentTransactions
            TwoPCProtocol.ControllerCommittedResp p = new TwoPCProtocol.ControllerCommittedResp(transId, myId);
            this.ms.sendAsync(address, TwoPCProtocol.ControllerCommittedResp.class.getName(), s.encode(p));
        }
        else {
            commit.accept(pendentKeys);

            this.log.write(transId, Phase.COMMITTED.toString());
            TwoPCProtocol.ControllerCommittedResp p = new TwoPCProtocol.ControllerCommittedResp(transId, myId);
            this.ms.sendAsync(address, TwoPCProtocol.ControllerCommittedResp.class.getName(), s.encode(p));
        }
    }
}
