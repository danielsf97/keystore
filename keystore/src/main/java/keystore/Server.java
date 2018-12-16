package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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



    private final Address myAddress = Address.from("localhost", 12350);
    private static ManagedMessagingService ms;
    private static Serializer s ;
    private static Serializer sp ;
    private ExecutorService es;

    private HashMap<Integer, Transaction> currentTransactions;
    private HashMap<Integer, CompletableFuture> currentFutures;
    private static AtomicInteger nextTxId = new AtomicInteger(0);


    public Server() {

        this.s = KeystoreProtocol
                .newSerializer();
        this.sp = TwoPCProtocol.newSerializer();

        this.es = Executors.newSingleThreadExecutor();

        this.ms =  NettyMessagingService.builder()
                .withAddress(myAddress)
                .build();

        this.currentFutures = new HashMap<>();
        this.currentTransactions = new HashMap<>();

        this.ms.registerHandler("put", (c, m) ->{
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

        ms.registerHandler(TwoPCProtocol.ControllerPreparedResp.class.getName(), (o,m) -> {
            System.out.println("Response from keystoresrv");
            processTPC1(m);
        },es);

        ms.registerHandler(TwoPCProtocol.ControllerCommitResp.class.getName(), (o,m) -> {
            processTPC2(m);
        },es);

       // this.coordinator = new Coordinator(this.ms, this.es, addresses);

        this.ms.start();
    }





    private void processPutReq(Transaction tx, KeystoreProtocol.PutReq req) {
        Map<Long,byte[]> values = req.values;
        Map<Integer, Map<Long, byte []>> separatedValues = valuesSeparator(values);
        tx.setParticipants(separatedValues.keySet());
        initTPC1(tx.getId(), separatedValues);
    }

    private Map<Integer, Map<Long,byte[]>> valuesSeparator(Map<Long,byte[]> values) {
        Map<Integer, Map<Long, byte []>> res = new HashMap<>();
        for(Map.Entry value: values.entrySet()){
            int ks = (int) ((long) value.getKey() % (long) addresses.length);
            System.out.println("Participante " + ks);
            if(!res.containsKey(ks)) res.put(ks, new HashMap<>());
            Map ksMap = res.get(ks);
            ksMap.put(value.getKey(), value.getValue());
            res.put(ks, ksMap);
        }
        return res;
    }


    public void initTPC1(int txId, Map<Integer, Map<Long,byte[]>> separatedValues) {

        for (Map.Entry<Integer, Map<Long, byte[]>> ksValues : separatedValues.entrySet()) {
            int participant = (int) ksValues.getKey();
            System.out.println("Enviado para o participante" + participant);
            TwoPCProtocol.ControllerPreparedReq contReq = new TwoPCProtocol.ControllerPreparedReq(txId, participant, ksValues.getValue());
            ms.sendAsync(addresses[ participant],
                    TwoPCProtocol.ControllerPreparedReq.class.getName(),
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
                };
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
            TwoPCProtocol.ControllerCommitReq contReq = new TwoPCProtocol.ControllerCommitReq(txId, pId);
            ms.sendAsync(addresses[pId],
                    TwoPCProtocol.ControllerCommitReq.class.getName(),
                    sp.encode(contReq));
        }
    }


    private void processTPC2(byte[] m){
        TwoPCProtocol.ControllerPreparedResp rp = sp.decode(m);
        Transaction e = currentTransactions.get(rp.txId);
        if (rp.resp == TwoPCProtocol.Status.COMMITED){
            e.setParticipant_resp(rp.pId, rp.resp);
            if (e.check_prepared()){
                e.setPhase(Transaction.Phase.COMMITED);
                KeystoreProtocol.PutResp p = new KeystoreProtocol.PutResp(true);
                currentFutures.get(rp.txId).complete(s.encode(p));
                currentFutures.remove(rp.txId);

            };
        }
        else if (rp.resp == TwoPCProtocol.Status.ABORT){
            e.setPhase(Transaction.Phase.ROLLBACKED);
            //ABORT NO LOG
        }
    }


    public static void main(String[] args) throws Exception {
        Server server = new Server();
    }
}
