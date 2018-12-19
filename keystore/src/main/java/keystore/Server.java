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
    private Log log;

    private HashMap<Integer, Transaction> currentTransactions;
    private HashMap<Integer, CompletableFuture<byte[]>> currentFutures;
    private HashMap<Integer, HashMap<Integer, CompletableFuture<Void>>> currentFutures2;

    private static AtomicInteger nextTxId = new AtomicInteger(0);


    private Server() {

        s = KeystoreProtocol
                .newSerializer();
        sp = TwoPCProtocol.newSerializer();

        ExecutorService es = Executors.newSingleThreadExecutor();

        Address myAddress = Address.from("localhost", 12350);

        ms =  NettyMessagingService.builder()
                .withAddress(myAddress)
                .build();

        this.log = new Log("Server");

        this.currentFutures = new HashMap<>();
        this.currentTransactions = new HashMap<>();
        this.currentFutures2 = new HashMap<>();

        ms.registerHandler("put", (c, m) -> {
                    System.out.println("HELLOOOOOO FROM SERVER");
                    KeystoreProtocol.PutReq req = s.decode(m);
                    CompletableFuture<byte[]> cf = new CompletableFuture<>();

                    //AQUI O CENAS NÂO ESTÀ A 0 NO INICIO
                    int txId = nextTxId.incrementAndGet();
                    currentFutures.put(txId, cf);
                    Transaction trans = new Transaction(txId);
                    currentTransactions.put(txId, trans);

                    processPutReq(trans, req);

                    //  this.log.read();

                    return cf;
        });

        ms.registerHandler("get", (c,m) -> {
            CompletableFuture<byte []> cf = new CompletableFuture<>();

            KeystoreProtocol.GetReq req = s.decode(m);

            //AQUI O CENAS NÂO ESTÀ A 0 NO INICIO
            int txId = nextTxId.incrementAndGet();
            currentFutures.put(txId, cf);
            Transaction trans = new Transaction(txId);
            currentTransactions.put(txId, trans);

            processGetReq(trans,req);
            return cf;

        });

        ms.registerHandler(TwoPCProtocol.ControllerPreparedResp.class.getName(), (o,m) -> {
            System.out.println("Response from keystoresrv");
            processTPC1(o,m);
        }, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommitResp.class.getName(), (o,m) -> {
            processTPC2(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.GetControllerResp.class.getName(),(o,m)->{
            processGetResp(m);
        },es);

        ms.registerHandler(TwoPCProtocol.Abort.class.getName(), this::abort, es);

        // this.coordinator = new Coordinator(this.ms, this.es, addresses);

        ms.start();
    }

    /////////////////////////ABORT///////////////////////////
    private void abort(Address address, byte[] bytes) {
        TwoPCProtocol.Abort abort = sp.decode(bytes);
        abort_complete(abort.txId);
    }

    private synchronized void abort_complete(int txId){
        Transaction e = currentTransactions.get(txId);

        if (e.getPhase()!= Transaction.Phase.ROLLBACKED){
            e.setPhase(Transaction.Phase.ROLLBACKED);

            TwoPCProtocol.Abort abortt = new TwoPCProtocol.Abort(txId);
            System.out.println("ABOOOOOOOOOOORRRTTTTTTTTTTT");
            for ( Integer pId :e.getParticipants()){
                ms.sendAsync(addresses[pId], TwoPCProtocol.Abort.class.getName(),sp.encode(abortt) );
            }
            System.out.println("ABOOOOOOOOOOORRRTTTTTTTTTTT2");

            currentFutures.get(txId).complete(s.encode(new KeystoreProtocol.PutResp(false)));
            System.out.println("ABOOOOOOOOOOORRRTTTTTTTTTTT3");
        }
    }

    /////////////////////////GET///////////////////////////

    private void processGetResp(byte[] m) {
        TwoPCProtocol.GetControllerResp rp = sp.decode(m);
        Transaction e = currentTransactions.get(rp.txId);
        e.setParticipant_resp(rp.pId,TwoPCProtocol.Status.COMMITED);
        if (e.check_commit()){
            e.setPhase(Transaction.Phase.COMMITED);
            KeystoreProtocol.GetResp p = new KeystoreProtocol.GetResp(rp.values);
            System.out.println(currentFutures.size());
            currentFutures.remove(rp.txId).complete(s.encode(p));
            System.out.println(currentFutures.size());
        }
    }


    private void processGetReq(Transaction tx, KeystoreProtocol.GetReq req){
        Collection<Long> keys = req.keys;
        Map<Integer, Collection<Long>> separatedValues = valuesSeparator(keys);
        tx.setParticipants(separatedValues.keySet());
        get(tx.getId(), separatedValues);
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
        log.write(tx.getId(),req.values);
        Map<Long,byte[]> values = req.values;
        Map<Integer, Map<Long, byte []>> separatedValues = valuesSeparator(values);
        tx.setParticipants(separatedValues.keySet());
        initPutTPC1(tx.getId(), separatedValues);
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




    private void initPutTPC1(int txId, Map<Integer, Map<Long, byte[]>> separatedValues) {

        currentFutures2.put(txId,new HashMap<>());
        for (Map.Entry<Integer, Map<Long, byte[]>> ksValues : separatedValues.entrySet()) {
            int pId = ksValues.getKey();
            TwoPCProtocol.PutControllerPreparedReq contReq = new TwoPCProtocol.PutControllerPreparedReq(txId, pId, ksValues.getValue());
            String type = TwoPCProtocol.PutControllerPreparedReq.class.getName();
            send_iteractive(pId,txId,contReq,type);
           /* ms.sendAsync(addresses[pId],
                    type,
                    sp.encode(contReq));

            HashMap<Integer, CompletableFuture<Void>> arr = currentFutures2.get(txId);
            CompletableFuture<Void> cp = new CompletableFuture<>();
            arr.put(pId,cp);
            currentFutures2.put(txId, arr );

            CompletableFuture.supplyAsync(()-> check_response(cp, contReq,addresses[ pId], 5,type));*/
        }
    }


    private void initTPC2(Transaction e) {
        Collection<Integer> participants =  e.getParticipants();
        int txId = e.getId();
        for (Integer pId: participants){
            TwoPCProtocol.PutControllerCommitReq contReq = new TwoPCProtocol.PutControllerCommitReq(txId, pId);
            String type = TwoPCProtocol.PutControllerCommitReq.class.getName();
            send_iteractive(pId,txId,contReq,type);
           /* ms.sendAsync(addresses[pId],
                    type,
                    sp.encode(contReq));

            HashMap<Integer, CompletableFuture<Void>> arr = currentFutures2.get(txId);
            CompletableFuture<Void> cp = new CompletableFuture<>();
            arr.put(pId,cp);
            currentFutures2.put(txId, arr );

            CompletableFuture.supplyAsync(()-> check_response(cp, contReq,addresses[ pId], 5,type));*/
        }
    }

    private void send_iteractive(int pId, int txId, TwoPCProtocol.PutControllerReq contReq, String type){
        ms.sendAsync(addresses[pId],
                type,
                sp.encode(contReq));

        HashMap<Integer, CompletableFuture<Void>> arr = currentFutures2.get(txId);
        CompletableFuture<Void> cp = new CompletableFuture<>();
        arr.put(pId,cp);
        currentFutures2.put(txId, arr );

        CompletableFuture.supplyAsync(()-> check_response(cp, contReq,addresses[ pId], 5,type));
    }


    private CompletableFuture<Void> check_response(CompletableFuture<Void> cp,  TwoPCProtocol.PutControllerReq contReq, Address a, int rep,String type){
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
            if (!currentFutures2.get(contReq.txId).get(contReq.pId).isDone()){
                abort_complete(contReq.txId);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

   /* private CompletableFuture<Void> check_responseCommit(CompletableFuture<Void> cp,  TwoPCProtocol.PutControllerReq contReq, Address a, int rep, String type){
        System.out.println("Waiting for response. Remaning attempts:" + rep);
        if (rep > 0){
            try {
                cp.get(1, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                ms.sendAsync(a,
                        type,
                        sp.encode(contReq));
                System.out.println("HELLOoooooo");
                check_responseCommit(cp, contReq,a,--rep,type);
            }
        }
        else {
            if (!currentFutures2.get(contReq.txId).get(contReq.pId).isDone()){
                abort_complete(contReq.txId);
            }
        }
        return CompletableFuture.completedFuture(null);
    }*/



    private void processTPC1(Address o, byte[] m) {
        TwoPCProtocol.ControllerPreparedResp rp = sp.decode(m);

        Transaction e = currentTransactions.get(rp.txId);
        if (e.getPhase()!=Transaction.Phase.ROLLBACKED){
            if (rp.resp == TwoPCProtocol.Status.PREPARED_OK && e.getParticipantStatus(rp.pId) != TwoPCProtocol.Status.PREPARED_OK){
                e.setParticipant_resp(rp.pId, rp.resp);
                System.out.println(currentFutures2.get(rp.txId).size());
                currentFutures2.get(rp.txId).remove(rp.pId).complete(null);
                System.out.println(currentFutures2.get(rp.txId).size());
                if (e.check_prepared()){
                    log.write(rp.txId,Log.Phase.PREPARED.toString());
                    e.setPhase(Transaction.Phase.PREPARED);
                    initTPC2(e);
                }
            }
            else if (rp.resp == TwoPCProtocol.Status.ABORT){
               abort_complete(rp.txId);
            }
        }
    }





//    private HashMap<Integer, HashMap<Integer, CompletableFuture<Void>>> currentFutures2;
    private void processTPC2(byte[] m){
        System.out.println("A acabar");
        TwoPCProtocol.ControllerCommitResp rp = sp.decode(m);

        Transaction e = currentTransactions.get(rp.txId);
        System.out.println(rp.resp.toString());
        if (rp.resp == TwoPCProtocol.Status.COMMITED && e.getParticipantStatus(rp.pId) != TwoPCProtocol.Status.COMMITED){
            e.setParticipant_resp(rp.pId, rp.resp);
            System.out.println(currentFutures2.get(rp.txId).size());
            currentFutures2.get(rp.txId).remove(rp.pId).complete(null);
            System.out.println(currentFutures2.get(rp.txId).size());
            if (e.check_commit()){
                e.setPhase(Transaction.Phase.COMMITED);
                log.write(rp.txId,Log.Phase.COMMITED.toString());
                KeystoreProtocol.PutResp p = new KeystoreProtocol.PutResp(true);
                currentFutures.get(rp.txId).complete(s.encode(p));
                currentFutures.remove(rp.txId);
                System.out.println("Done");

            }
        }
        else if (rp.resp == TwoPCProtocol.Status.ABORT){
            abort_complete(rp.txId);
        }
    }


    public static void main(String[] args) {
        new Server();
    }
}
