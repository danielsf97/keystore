package keystore;

import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import tpc.Coordinator;
import tpc.Phase;
import tpc.TwoPCTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;


public class Server extends Serv {

    private static final Address[] addresses = new Address[] {
            Address.from("localhost", 12345),
            Address.from("localhost", 12346),
            Address.from("localhost", 12347)
    };


    private static Serializer s = KeystoreProtocol
            .newSerializer();
    private static Serializer sp = Server_KeystoreSrvProtocol.newSerializer();

    private HashMap<Integer, Transaction> currentGets;
    private ReentrantLock currentGetsGlobalLock;
    private AtomicInteger nextGetId;
    private Coordinator coordinator;

    //private static AtomicInteger nextTxId;


    private Server() {
        super(Address.from("localhost", 12350));


        ms.start();

        BiConsumer<Boolean,TwoPCTransaction> whenDone = (aBoolean, twoPCTransaction) -> {
            KeystoreProtocol.PutResp p = new KeystoreProtocol.PutResp( aBoolean, twoPCTransaction.get_client_txId());
            ms.sendAsync(twoPCTransaction.getAddress(), KeystoreProtocol.PutResp.class.getName(), s.encode(p));
        };



        coordinator = new Coordinator<Map<Long,byte[]>>(addresses,ms,whenDone,"Server",es);



        this.currentGets = new HashMap<>();
        this.currentGetsGlobalLock = new ReentrantLock();
        this.nextGetId =  new AtomicInteger(0);



        ms.registerHandler("put", (c, m) -> {
            System.out.println("HELLOOOOOO FROM SERVER");
            KeystoreProtocol.PutReq req = s.decode(m);

            //AQUI O CENAS NÂO ESTÀ A 0 NO INICIO

            Map<Long,byte[]> values = req.values;
            Map<Integer, Map<Long, byte []>> separatedValues = valuesSeparator(values);

            coordinator.initProcess(req.txId,c,separatedValues);


        },es);

        ms.registerHandler("get", (c,m) -> {

            KeystoreProtocol.GetReq req = s.decode(m);

            //AQUI O CENAS NÂO ESTÀ A 0 NO INICIO
            int txId = nextGetId.incrementAndGet();
            Transaction trans = new Transaction(txId,req.txId, c);

            this.currentGetsGlobalLock.lock();
            currentGets.put(txId, trans);
            this.currentGetsGlobalLock.unlock();

            processGetReq(trans,req);

        },es);




        ms.registerHandler(Server_KeystoreSrvProtocol.GetControllerResp.class.getName(),(o,m)->{
            processGetResp(m);
        },es);



        ms.start();

    }






    /////////////////////////GET///////////////////////////

    private void processGetResp(byte[] m) {
        Server_KeystoreSrvProtocol.GetControllerResp rp = sp.decode(m);

        currentGetsGlobalLock.lock();
        Transaction e = currentGets.get(rp.txId);
        currentGetsGlobalLock.unlock();

        if (e != null) {
            e.lock();
            if (e.getParticipantStatus(rp.pId) != Phase.COMMITTED) {
                e.setParticipantStatus(rp.pId, Phase.COMMITTED);
                e.setKeys(rp.values);
            }
            if (e.checkParticipantsPhases(Phase.COMMITTED)) {
                e.setPhase(Phase.COMMITTED);
                e.unlock();
                KeystoreProtocol.GetResp p = new KeystoreProtocol.GetResp(e.getKeys(), e.get_client_txId());
                ms.sendAsync(e.getAddress(),KeystoreProtocol.GetResp.class.getName(),s.encode(p));

                currentGetsGlobalLock.lock();
                currentGets.remove(rp.txId);
                currentGetsGlobalLock.unlock();
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
            Server_KeystoreSrvProtocol.GetControllerReq contReq = new Server_KeystoreSrvProtocol.GetControllerReq(txId, participant, ksValues.getValue());
            ms.sendAsync(addresses[ participant],
                    Server_KeystoreSrvProtocol.GetControllerReq.class.getName(),
                    sp.encode(contReq));
        }
    }

    /////////////////////////PUT///////////////////////////


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




    public static void main(String[] args) {
        new Server();
    }


}
