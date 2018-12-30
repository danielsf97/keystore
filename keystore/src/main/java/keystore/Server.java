package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class Server {

    private static final Address[] addresses = new Address[] {
            Address.from("localhost", 12345),
            Address.from("localhost", 12346),
            Address.from("localhost", 12347)
    };


    private static ManagedMessagingService ms;
    private static Serializer s ;
    private static Serializer sp ;
    private Log<Object> log;

    private HashMap<Integer, Transaction> currentTransactions;
    private ReentrantLock currentTransactionsGlobalLock;

    private static AtomicInteger nextTxId;


    private Server() {

        s = KeystoreProtocol
                .newSerializer();
        sp = TwoPCProtocol.newSerializer();

        ExecutorService es = Executors.newSingleThreadExecutor();

        Address myAddress = Address.from("localhost", 12350);

        ms =  NettyMessagingService.builder()
                .withAddress(myAddress)
                .build();

        this.log = new Log<>("Server");



        this.currentTransactions = new HashMap<>();
        this.currentTransactionsGlobalLock = new ReentrantLock();
        this.nextTxId =  new AtomicInteger(0);

        System.out.println("Size: " + currentTransactions.size());

        restore();

        ms.registerHandler("put", (c, m) -> {
            System.out.println("HELLOOOOOO FROM SERVER");
            KeystoreProtocol.PutReq req = s.decode(m);

            //AQUI O CENAS NÂO ESTÀ A 0 NO INICIO
            int txId = nextTxId.incrementAndGet();
            Transaction trans = new Transaction(txId,req.txId,c);

            this.currentTransactionsGlobalLock.lock();
            currentTransactions.put(txId, trans);
            this.currentTransactionsGlobalLock.unlock();

            processPutReq(trans, req);


            //  this.log.read();


          //  return cf;

        },es);

        ms.registerHandler("get", (c,m) -> {

            KeystoreProtocol.GetReq req = s.decode(m);

            //AQUI O CENAS NÂO ESTÀ A 0 NO INICIO
            int txId = nextTxId.incrementAndGet();
            Transaction trans = new Transaction(txId,req.txId, c);

            this.currentTransactionsGlobalLock.lock();
            currentTransactions.put(txId, trans);
            this.currentTransactionsGlobalLock.unlock();

            processGetReq(trans,req);

        },es);

        ms.registerHandler(TwoPCProtocol.ControllerPreparedResp.class.getName(), (o,m) -> {
            System.out.println("Response from keystoresrv");
            processTPC1(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommitedResp.class.getName(), (o, m) -> {
            processTPC2(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.GetControllerResp.class.getName(),(o,m)->{
            processGetResp(m);
        },es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortReq.class.getName(),(o,m)->{
            processAbortReq(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortResp.class.getName(),(o,m)->{
            processAbortResp(m);
        }, es);

        // this.coordinator = new Coordinator(this.ms, this.es, addresses);

        ms.start();

    }


    private void restore() {
        HashMap<Integer,SimpleTransaction> keys = new HashMap<>();
        HashMap<Integer,String> phases = new HashMap<>();
        boolean not_null = true;
        int i = 0;
        while(not_null){
            Log.LogEntry e = log.read(i);
            if (e==null) not_null = false;
            else {
                System.out.println(e.toString());
                Object o = e.getAction();
                System.out.println("NOME: " + o.getClass().getName());
                if (o instanceof SimpleTransaction) {
                    keys.put(e.getTrans_id(), (SimpleTransaction) o);
                }
                else {
                    phases.put(e.getTrans_id(), (String) o );
                }
            }
            i++;
        }
        for(Map.Entry<Integer,String> entry : phases.entrySet()){
            if (entry.getValue().equals(Phase.STARTED.toString())){
                nextTxId.incrementAndGet();
                SimpleTransaction tx = keys.get(entry.getKey());
                Transaction trans = new Transaction(tx, Phase.STARTED);
                currentTransactions.put(trans.getId(), trans);
                initPutTPC1(trans.getId(),tx.participantsToKeys);

            }
            else if (entry.getValue().equals(Phase.PREPARED.toString())){
                SimpleTransaction tx = keys.get(entry.getKey());
                nextTxId.incrementAndGet();
                Transaction trans = new Transaction(tx, Phase.PREPARED);
                currentTransactions.put(trans.getId(), trans);
                initTPC2(trans);
            }
            else if (entry.getValue().equals(Phase.ABORT.toString())){
                SimpleTransaction tx = keys.get(entry.getKey());
                nextTxId.incrementAndGet();
                Transaction trans = new Transaction(tx, Phase.PREPARED);
                currentTransactions.put(trans.getId(), trans);
                initAbort(trans.getId());
            }
            else{
                nextTxId.incrementAndGet();
            }
        }

        nextTxId.addAndGet((int) log.last_index());
        System.out.println("IIIIIIIIIIIIIIIIIII: " + nextTxId.get());
    }

    /////////////////////////ABORT///////////////////////////




    private void processAbortReq(byte[] m) {
        TwoPCProtocol.ControllerAbortReq rp = sp.decode(m);

        this.currentTransactionsGlobalLock.lock();
        Transaction e = currentTransactions.get(rp.txId);
        this.currentTransactionsGlobalLock.unlock();

        e.lock();
        e.setParticipantStatus(rp.pId, Phase.ROLLBACKED);
        initAbort(e.getId());
        e.unlock();
    }




    /////////////////////////GET///////////////////////////

    private void processGetResp(byte[] m) {
        TwoPCProtocol.GetControllerResp rp = sp.decode(m);

        currentTransactionsGlobalLock.lock();
        Transaction e = currentTransactions.get(rp.txId);
        currentTransactionsGlobalLock.unlock();

        if (e != null) {
            e.lock();
            if (e.getParticipantStatus(rp.pId) != Phase.COMMITED) {
                e.setParticipantStatus(rp.pId, Phase.COMMITED);
                e.setKeys(rp.values);
            }
            if (e.checkParticipantsPhases(Phase.COMMITED)) {
                e.setPhase(Phase.COMMITED);
                e.unlock();
                KeystoreProtocol.GetResp p = new KeystoreProtocol.GetResp(e.getKeys(), e.get_client_txId());
                ms.sendAsync(e.getAddress(),KeystoreProtocol.GetResp.class.getName(),s.encode(p));

                currentTransactionsGlobalLock.lock();
                currentTransactions.remove(rp.txId);
                currentTransactionsGlobalLock.unlock();
            }else{
                e.unlock();
            }
        }
    }


    private void processGetReq(Transaction tx, KeystoreProtocol.GetReq req){
        Collection<Long> keys = req.keys;
        Map<Integer, Collection<Long>> separatedValues = valuesSeparator(keys);
        tx.setParticipants(separatedValues.keySet());
        initget(tx.getId(), separatedValues);
    }

    private Map<Integer,Collection<Long>> valuesSeparator(Collection<Long> keys){
        Map<Integer, Collection<Long>> res = new HashMap<>();
        for(Long key : keys){
            int ks = (int) (key % (long) addresses.length);
            System.out.println("Participante " + ks);
            if(!res.containsKey(ks)) res.put(ks, new ArrayList<>());
            Collection<Long> ksCol = res.get(ks);
            ksCol.add(key);
            res.put(ks, ksCol);
        }
        return res;
    }



    private void initget(int txId, Map<Integer, Collection<Long>> separatedValues) {

        for (Map.Entry<Integer, Collection<Long>> ksValues : separatedValues.entrySet()) {
            int participant = ksValues.getKey();
            System.out.println("Enviado para o participante" + participant);
            TwoPCProtocol.GetControllerReq contReq = new TwoPCProtocol.GetControllerReq(txId, participant, ksValues.getValue());
            ms.sendAsync(addresses[ participant],
                    TwoPCProtocol.GetControllerReq.class.getName(),
                    sp.encode(contReq));
        }
    }

    /////////////////////////PUT///////////////////////////

    private void processPutReq(Transaction tx, KeystoreProtocol.PutReq req) {
        Map<Long,byte[]> values = req.values;
        Map<Integer, Map<Long, byte []>> separatedValues = valuesSeparator(values);
        tx.setParticipants(separatedValues.keySet());
        SimpleTransaction simpleTx = new SimpleTransaction(tx.getId(),tx.get_client_txId(),tx.getAddress(),separatedValues);
        log.write(tx.getId(),simpleTx);
        log.write(tx.getId(),Phase.STARTED.toString());
        initPutTPC1(tx.getId(), separatedValues);
    }



    private Map<Integer, Map<Long,byte[]>> valuesSeparator(Map<Long,byte[]> values) {
        Map<Integer, Map<Long, byte []>> res = new HashMap<>();
        for(Map.Entry<Long, byte []> value: values.entrySet()){
            int ks = (int) (value.getKey() % (long) addresses.length);
            System.out.println("Participante " + ks);
            if(!res.containsKey(ks)) res.put(ks, new HashMap<>());
            Map<Long, byte []> ksMap = res.get(ks);
            ksMap.put(value.getKey(), value.getValue());
            res.put(ks, ksMap);
        }
        return res;
    }



    private void initPutTPC1(int txId, Map<Integer, Map<Long, byte[]>> separatedValues) {

        for (Map.Entry<Integer, Map<Long, byte[]>> ksValues : separatedValues.entrySet()) {
            int pId = ksValues.getKey();
            TwoPCProtocol.ControllerPreparedReq contReq = new TwoPCProtocol.ControllerPreparedReq(txId, pId, ksValues.getValue());
            String type = TwoPCProtocol.ControllerPreparedReq.class.getName();
            send_iteractive(pId,txId,contReq,type);

        }
    }



    private void initTPC2(Transaction e) {
        Collection<Integer> participants =  e.getParticipants();
        int txId = e.getId();
        for (Integer pId: participants){
            TwoPCProtocol.ControllerCommitReq contReq = new TwoPCProtocol.ControllerCommitReq(txId, pId);
            String type = TwoPCProtocol.ControllerCommitReq.class.getName();

            send_iteractive2(pId,txId,contReq,type, Phase.COMMITED);

        }
    }

    private synchronized void initAbort(int txId){

        currentTransactionsGlobalLock.lock();
        Transaction e = currentTransactions.get(txId);
        currentTransactionsGlobalLock.unlock();

        e.lock();
        if (e.getPhase() != Phase.ROLLBACKED && e.getPhase() != Phase.ABORT) {

            e.setPhase(Phase.ABORT);
            e.unlock();

            log.write(txId, Phase.ABORT.toString());

            KeystoreProtocol.PutResp p = new KeystoreProtocol.PutResp(false, e.get_client_txId());
            ms.sendAsync(e.getAddress(), KeystoreProtocol.PutResp.class.getName(), s.encode(p));

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


    private void send_iteractive(int pId, int txId, TwoPCProtocol.ControllerReq contReq, String type){
        ms.sendAsync(addresses[pId],
                type,
                sp.encode(contReq));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(()->{
            System.out.println("CHECKING");
            currentTransactionsGlobalLock.lock();
            Transaction e = currentTransactions.get(txId);
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
            Transaction e = currentTransactions.get(txId);
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


        }, 0, 5, TimeUnit.SECONDS);
    }

    private void processTPC1(byte[] m) {
        TwoPCProtocol.ControllerPreparedResp rp = sp.decode(m);

        this.currentTransactionsGlobalLock.lock();
        Transaction e = currentTransactions.get(rp.txId);
        this.currentTransactionsGlobalLock.unlock();

        e.lock();
        if (e.getPhase() != Phase.ABORT){
            System.out.println("Init Commit: " + rp.pId);
            e.setParticipantStatus(rp.pId, Phase.PREPARED);
            if (e.checkParticipantsPhases(Phase.PREPARED)){
                e.setPhase(Phase.PREPARED);
                e.unlock();

                log.write(rp.txId,Phase.PREPARED.toString());
                KeystoreProtocol.PutResp p = new KeystoreProtocol.PutResp(true, e.get_client_txId());
                ms.sendAsync(e.getAddress(), KeystoreProtocol.PutResp.class.getName(), s.encode(p));
                System.out.println("Init Commit");
                initTPC2(e);
            }else e.unlock();
        }else e.unlock();

    }

    private void processTPC2(byte[] m){
        System.out.println("A acabar");
        TwoPCProtocol.ControllerCommitedResp rp = sp.decode(m);

        this.currentTransactionsGlobalLock.lock();
        if (currentTransactions.containsKey(rp.txId)) {

            Transaction e = currentTransactions.get(rp.txId);
            this.currentTransactionsGlobalLock.unlock();

            e.lock();
            if (e.getPhase() != Phase.ABORT) {
                e.setParticipantStatus(rp.pId, Phase.COMMITED);
                if (e.checkParticipantsPhases(Phase.COMMITED)) {
                    e.setPhase(Phase.COMMITED);
                    e.unlock();

                    this.currentTransactionsGlobalLock.lock();
                    currentTransactions.remove(rp.txId);
                    this.currentTransactionsGlobalLock.unlock();

                    log.write(rp.txId, Phase.COMMITED.toString());
                    System.out.println("Done");
                }else e.unlock();
            }else e.unlock();

        }else{
            this.currentTransactionsGlobalLock.unlock();
        }
    }

    private void processAbortResp(byte[] bytes) {
        TwoPCProtocol.ControllerAbortResp rp = sp.decode(bytes);

        this.currentTransactionsGlobalLock.lock();
        if (currentTransactions.containsKey(rp.txId)) {

            Transaction e = currentTransactions.get(rp.txId);
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


    public static void main(String[] args) {
        new Server();
    }
}
