package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    private HashMap<Integer, Transaction> currentTransactions;
    private HashMap<Integer, CompletableFuture<byte[]>> currentFutures;
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

        this.currentFutures = new HashMap<>();
        this.currentTransactions = new HashMap<>();

        ms.registerHandler("put", (c, m) ->{
            System.out.println("HELLOOOOOO FROM SERVER");
            KeystoreProtocol.PutReq req = s.decode(m);
            CompletableFuture<byte []> cf = new CompletableFuture<>();

            //AQUI O CENAS NÂO ESTÀ A 0 NO INICIO
            int txId = nextTxId.incrementAndGet();
            currentFutures.put(txId, cf);
            Transaction trans = new Transaction(txId);
            currentTransactions.put(txId, trans);

            processPutReq(trans, req);

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
            processTPC1(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommitResp.class.getName(), (o,m) -> {
            processTPC2(m);
        }, es);

        ms.registerHandler(TwoPCProtocol.GetControllerResp.class.getName(),(o,m)->{
            processGetResp(m);
        },es);

       // this.coordinator = new Coordinator(this.ms, this.es, addresses);

        ms.start();
    }

    private void processGetResp(byte[] m) {
        TwoPCProtocol.GetControllerResp rp = sp.decode(m);
        Transaction e = currentTransactions.get(rp.txId);
        e.setParticipant_resp(rp.pId,TwoPCProtocol.Status.COMMITED);
        if (e.check_commit()){
            e.setPhase(Transaction.Phase.COMMITED);
            KeystoreProtocol.GetResp p = new KeystoreProtocol.GetResp(rp.values);
            currentFutures.get(rp.txId).complete(s.encode(p));
            currentFutures.remove(rp.txId);
        }
    }


    private void processGetReq(Transaction tx, KeystoreProtocol.GetReq req){
        Collection<Long> keys = req.keys;
        Map<Integer, Collection<Long>> separatedValues = valuesSeparator(keys);
        tx.setParticipants(separatedValues.keySet());
        get(tx.getId(), separatedValues);
    }


    private void processPutReq(Transaction tx, KeystoreProtocol.PutReq req) {
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
            int participant = ksValues.getKey();
            System.out.println("Enviado para o participante" + participant);
            TwoPCProtocol.PutControllerPreparedReq contReq = new TwoPCProtocol.PutControllerPreparedReq(txId, participant, ksValues.getValue());
            ms.sendAsync(addresses[ participant],
                    TwoPCProtocol.PutControllerPreparedReq.class.getName(),
                    sp.encode(contReq));
        }
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

    private void processTPC1(byte[] m) {
        TwoPCProtocol.ControllerPreparedResp rp = sp.decode(m);
        Transaction e = currentTransactions.get(rp.txId);
        if (e.getPhase()!=Transaction.Phase.ROLLBACKED){
            if (rp.resp == TwoPCProtocol.Status.PREPARED_OK){
                e.setParticipant_resp(rp.pId, rp.resp);
                if (e.check_prepared()){
                    e.setPhase(Transaction.Phase.PREPARED);
                    initTPC2(e);
                }
            }
            else if (rp.resp == TwoPCProtocol.Status.ABORT){
                e.setPhase(Transaction.Phase.ROLLBACKED);
                //ABORT NO LOG
            }
        }
    }

    private void initTPC2(Transaction e) {
        Collection<Integer> participants =  e.getParticipants();
        int txId = e.getId();
        for (Integer pId: participants){
            TwoPCProtocol.PutControllerCommitReq contReq = new TwoPCProtocol.PutControllerCommitReq(txId, pId);
            ms.sendAsync(addresses[pId],
                    TwoPCProtocol.PutControllerCommitReq.class.getName(),
                    sp.encode(contReq));
        }
    }


    private void processTPC2(byte[] m){
        System.out.println("A acabar");
        TwoPCProtocol.ControllerCommitResp rp = sp.decode(m);
        Transaction e = currentTransactions.get(rp.txId);
        System.out.println(rp.resp.toString());
        if (rp.resp == TwoPCProtocol.Status.COMMITED){
            e.setParticipant_resp(rp.pId, rp.resp);
            if (e.check_commit()){
                e.setPhase(Transaction.Phase.COMMITED);
                KeystoreProtocol.PutResp p = new KeystoreProtocol.PutResp(true);
                currentFutures.get(rp.txId).complete(s.encode(p));
                currentFutures.remove(rp.txId);
                System.out.println("Done");

            }
        }
        else if (rp.resp == TwoPCProtocol.Status.ABORT){
            e.setPhase(Transaction.Phase.ROLLBACKED);
            //ABORT NO LOG
        }
    }


    public static void main(String[] args) {
        new Server();
    }
}
