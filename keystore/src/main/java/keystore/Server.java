package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
    private HashMap<Integer, HashMap<Integer, CompletableFuture<Void>>> currentFuturesP1;
    private HashMap<Integer, HashMap<Integer, CompletableFuture<Void>>> currentFuturesP2;
    private HashMap<Integer, HashMap<Integer, CompletableFuture<Void>>> currentFuturesA;

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
        this.currentFuturesP1 = new HashMap<>();
        this.currentFuturesP2 = new HashMap<>();
        this.currentFuturesA = new HashMap<>();
        this.nextTxId =  new AtomicInteger(0);

        System.out.println("Size: " + currentTransactions.size());

        ms.registerHandler("put", (c, m) -> {
            System.out.println("HELLOOOOOO FROM SERVER");
            KeystoreProtocol.PutReq req = s.decode(m);

            //AQUI O CENAS NÂO ESTÀ A 0 NO INICIO
            int txId = nextTxId.incrementAndGet();
            Transaction trans = new Transaction(txId,req.txId,c);
            currentTransactions.put(txId, trans);

            processPutReq(trans, req);


            //  this.log.read();


          //  return cf;

        },es);

        ms.registerHandler("get", (c,m) -> {
            CompletableFuture<byte []> cf = new CompletableFuture<>();

            KeystoreProtocol.GetReq req = s.decode(m);

            //AQUI O CENAS NÂO ESTÀ A 0 NO INICIO
            int txId = nextTxId.incrementAndGet();
            Transaction trans = new Transaction(txId,2, Address.from("localhost", 12346));
            currentTransactions.put(txId, trans);

            processGetReq(trans,req);
            return cf;

        });

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
            processAbort2(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortResp.class.getName(),(o,m)->{
            processAbort(m);
        }, es);

        // this.coordinator = new Coordinator(this.ms, this.es, addresses);

        ms.start();
        restore();

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

        System.out.println("IIIIIIIIIIIIIIIIIII: " + nextTxId.get());
    }

    /////////////////////////ABORT///////////////////////////




    private void processAbort2(byte[] m) {
        TwoPCProtocol.ControllerAbortReq rp = sp.decode(m);
        Transaction e = currentTransactions.get(rp.txId);
        e.setParticipant_resp(rp.pId, Phase.ROLLBACKED);
        initAbort(e.getId());

    }




    /////////////////////////GET///////////////////////////

    private void processGetResp(byte[] m) {
        TwoPCProtocol.GetControllerResp rp = sp.decode(m);
        Transaction e = currentTransactions.get(rp.txId);
        e.setParticipant_resp(rp.pId,Phase.COMMITED);
        if (e.check_phase(Phase.COMMITED)){
            e.setPhase(Phase.COMMITED);
            KeystoreProtocol.GetResp p = new KeystoreProtocol.GetResp(rp.values);

        }
    }


    private void processGetReq(Transaction tx, KeystoreProtocol.GetReq req){
        Collection<Long> keys = req.keys;
        Map<Integer, Collection<Long>> separatedValues = valuesSeparator(keys);
        tx.setParticipants(separatedValues.keySet());
        get(tx.getId(), separatedValues);
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



    private void get(int txId, Map<Integer, Collection<Long>> separatedValues) {

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

        currentFuturesP1.put(txId,new HashMap<>());

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
        currentFuturesP2.put(txId,new HashMap<>());
        for (Integer pId: participants){
            TwoPCProtocol.ControllerCommitReq contReq = new TwoPCProtocol.ControllerCommitReq(txId, pId);
            String type = TwoPCProtocol.ControllerCommitReq.class.getName();
            send_iteractive2(pId,txId,contReq,type, currentFuturesP2);

        }
    }

    private synchronized void initAbort(int txId){
        Transaction e = currentTransactions.get(txId);
        currentFuturesA.put(txId,new HashMap<>());

        if (e.getPhase()!= Phase.ROLLBACKED){
            log.write(txId,Phase.ABORT.toString());
            e.setPhase(Phase.ABORT);
            KeystoreProtocol.PutResp p = new KeystoreProtocol.PutResp(false, e.get_client_txId());
            ms.sendAsync(e.getAddress(), KeystoreProtocol.PutResp.class.getName(), s.encode(p));

            String type = TwoPCProtocol.ControllerAbortReq.class.getName();
            for ( Integer pId :e.getParticipants()){
                TwoPCProtocol.ControllerAbortReq abortt = new TwoPCProtocol.ControllerAbortReq(txId,pId);
                ms.sendAsync(addresses[pId], type ,sp.encode(abortt) );
                send_iteractive2(pId,txId,abortt,type, currentFuturesA);
            }
        }
    }


    private void send_iteractive(int pId, int txId, TwoPCProtocol.ControllerReq contReq, String type){
        ms.sendAsync(addresses[pId],
                type,
                sp.encode(contReq));

      /*  HashMap<Integer, CompletableFuture<Void>> arr = currentFuturesP1.get(txId);
        CompletableFuture<Void> cp = new CompletableFuture<>();
        arr.put(pId,cp);
        currentFuturesP1.put(txId, arr );
        System.out.println("TAMANHO:" + currentFuturesP1.get(txId).size());*/

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(()->{
            check(txId,pId,Phase.PREPARED, scheduler);
        }, 5, 5, TimeUnit.SECONDS);

      //  CompletableFuture.supplyAsync(()-> check_response(cp, contReq,addresses[ pId], 2,type));
    }

    public void check(int txId, int pId, Phase phase, ScheduledExecutorService scheduler){
            System.out.println("CHECKING");
            Transaction e = currentTransactions.get(txId);
            if (e.getParticipantStatus(pId) == phase){
               scheduler.shutdown();
            }
    }

    private void send_iteractive2(int pId, int txId, TwoPCProtocol.ControllerReq contReq, String type, HashMap<Integer, HashMap<Integer, CompletableFuture<Void>>> futures){
        ms.sendAsync(addresses[pId],
                type,
                sp.encode(contReq));

        HashMap<Integer, CompletableFuture<Void>> arr = futures.get(txId);
        CompletableFuture<Void> cp = new CompletableFuture<>();
        arr.put(pId,cp);
        futures.put(txId, arr );
        System.out.println("TAMANHO:" + futures.get(txId).size());

        CompletableFuture.supplyAsync(()-> check_response(cp, contReq,addresses[ pId],type));
    }




    private CompletableFuture<Void> check_response(CompletableFuture<Void> cp,  TwoPCProtocol.ControllerReq contReq, Address a, int rep,String type){
        System.out.println("Waiting for response. Remaning attempts:" + rep);
        if (rep > 0){
            try {
                cp.get(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                ms.sendAsync(a,
                        type,
                        sp.encode(contReq));
                System.out.println("HELLOoooooo");
                check_response(cp, contReq,a,--rep,type);
            }
        }
        else {
            if (!currentFuturesP1.get(contReq.txId).get(contReq.pId).isDone()){
                initAbort(contReq.txId);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> check_response(CompletableFuture<Void> cp,  TwoPCProtocol.ControllerReq contReq, Address a,String type){
        System.out.println("Waiting for response from:" + a.toString());
        try {
            cp.get(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            ms.sendAsync(a,
                    type,
                    sp.encode(contReq));
            System.out.println("HELLOoooooo");
            check_response(cp, contReq,a,type);
        }
        return CompletableFuture.completedFuture(null);
    }


    private void processTPC1(byte[] m) {
        TwoPCProtocol.ControllerPreparedResp rp = sp.decode(m);
        Transaction e = currentTransactions.get(rp.txId);
        if (e.getPhase()!=Phase.ABORT){
            if (e.getParticipantStatus(rp.pId) != Phase.PREPARED){
                e.setParticipant_resp(rp.pId, Phase.PREPARED);
             //   currentFuturesP1.get(rp.txId).remove(rp.pId).complete(null);
                if (e.check_phase(Phase.PREPARED)){
                    e.setPhase(Phase.PREPARED);
                    log.write(rp.txId,Phase.PREPARED.toString());
                    KeystoreProtocol.PutResp p = new KeystoreProtocol.PutResp(true, e.get_client_txId());
                    ms.sendAsync(e.getAddress(), KeystoreProtocol.PutResp.class.getName(), s.encode(p));
                    initTPC2(e);
                }
            }
        }
    }

    private void processTPC2(byte[] m){
        System.out.println("A acabar");
        TwoPCProtocol.ControllerCommitedResp rp = sp.decode(m);
        Transaction e = currentTransactions.get(rp.txId);
        if (e.getPhase()!=Phase.ABORT) {
            if (e.getParticipantStatus(rp.pId) != Phase.COMMITED) {
                e.setParticipant_resp(rp.pId, Phase.COMMITED);
                currentFuturesP2.get(rp.txId).remove(rp.pId).complete(null);
                if (e.check_phase(Phase.COMMITED)) {
                    e.setPhase(Phase.COMMITED);
                    log.write(rp.txId, Phase.COMMITED.toString());
                    currentTransactions.remove(rp.txId);
                    // currentFutures.remove(rp.txId).complete(s.encode(p));
                    System.out.println("Done");
                }
            }
        }
    }

    private void processAbort(byte[] bytes) {
        TwoPCProtocol.ControllerAbortResp rp = sp.decode(bytes);
        Transaction e = currentTransactions.get(rp.txId);
        if (e.getPhase()!=Phase.ROLLBACKED) {
            System.out.println("FASE1:" + e.getParticipantStatus(rp.pId).toString());
            if (e.getParticipantStatus(rp.pId) != Phase.ROLLBACKED) {
                e.setParticipant_resp(rp.pId, Phase.ROLLBACKED);
                System.out.println("FASE2:" + e.getParticipantStatus(rp.pId).toString());
                System.out.println("HELO: " + currentFuturesA.get(rp.txId).size());
                currentFuturesA.get(rp.txId).remove(rp.pId).complete(null);
                if (e.check_phase(Phase.ROLLBACKED)) {
                    currentTransactions.remove(rp.txId);
                    System.out.println("All prepared for tx:" + e.getId());
                    e.setPhase(Phase.ROLLBACKED);
                    log.write(rp.txId, Phase.ROLLBACKED.toString());

                    System.out.println("ABOOOOOOOOOOORRRTTTTTTTTTTTEEDDD");
                }
            }
        }
    }


    public static void main(String[] args) {
        new Server();
    }
}
