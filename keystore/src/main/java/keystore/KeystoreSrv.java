package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KeystoreSrv  {


    private static final Address[] addresses = new Address[]{
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };

    private Scanner sc = new Scanner(System.in);
    private Map<Long, byte[]> data;
    private static ManagedMessagingService ms;
    private static Serializer s;
    private Log<Object> log;
    private int myId;

    private Map<Integer,Map<Long, byte[]>> pendent_puts;

    private KeystoreSrv(int id){


        this.myId = id;

        ms = NettyMessagingService.builder()
                .withAddress(addresses[myId])
                .build();
        s  = TwoPCProtocol
                .newSerializer();

        ExecutorService es = Executors.newSingleThreadExecutor();

        this.log = new Log<>("KeyStoreSrv" + id);

        this.pendent_puts = new HashMap<>();

        data = new HashMap<>();

        restore();

        //Recebe um pedido de prepared
        ms.registerHandler(TwoPCProtocol.ControllerPreparedReq.class.getName(), this::putTwoPC1, es);

        ms.registerHandler(TwoPCProtocol.ControllerCommitReq.class.getName(), this::putTwoPC2, es);

        ms.registerHandler(TwoPCProtocol.GetControllerReq.class.getName(), this::get, es);

        ms.registerHandler(TwoPCProtocol.ControllerAbortReq.class.getName(), this::abort, es);
        //TODO: aqui ir buscar a informação ao Log

        System.out.println("Size: " + data.size());

        ms.start();
    }

    private void restore() {
        HashMap<Integer,HashMap<Long,byte[]>> keys = new HashMap<>();
        HashMap<Integer,String> phases = new HashMap<>();
        int i = 0;
        boolean not_null = true;
        while(not_null){
            Log.LogEntry e = log.read(i);
            if (e==null) not_null = false;
            else {
                System.out.println(e.toString());
                Object o = e.getAction();
                if (o instanceof HashMap) {
                    keys.put(e.getTrans_id(), (HashMap<Long, byte[]>) o);
                }
                else {
                    phases.put(e.getTrans_id(), (String) o );
                }
            }
            i++;
        }
        for(Map.Entry<Integer,String> entry : phases.entrySet()){
            if (entry.getValue().equals(Phase.PREPARED.toString())){
                pendent_puts.put(entry.getKey(),keys.get(entry.getKey()));
            }
            else if (entry.getValue().equals(Phase.COMMITED.toString())){
                data.putAll(keys.get(entry.getKey()));
            }
        }
    }

    private void abort(Address address, byte[] bytes) {
        System.out.println("ABORT");
        TwoPCProtocol.ControllerAbortReq ab = s.decode(bytes);
        int trans_id = ab.txId;
        if(log.actionAlreadyExists(trans_id, Phase.ROLLBACKED)){

            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));
            System.out.println("Transaction " + trans_id + " already rollbacked!!");
        }
        else if (log.actionAlreadyExists(trans_id, Phase.PREPARED)) {
            log.write(trans_id, Phase.ROLLBACKED.toString());
            pendent_puts.remove(trans_id);
            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));
            System.out.println("Transaction " + trans_id + " rollbacked from prepared!!");
        }
        else {
            log.write(trans_id, Phase.ROLLBACKED.toString());
            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));
            System.out.println("Transaction " + trans_id + " rollbacked from commit!!");
        }
    }


    private void get(Address address, byte[] m) {
        System.out.println("PUT in keystore");
        TwoPCProtocol.GetControllerReq prepReq = s.decode(m);
        Collection<Long> keys = prepReq.keys;
        Map<Long,byte[]> rp = new HashMap<>();
        for(Long e : keys){
            if (data.containsKey(e))
                rp.put(e,data.get(e));
        }
        TwoPCProtocol.GetControllerResp resp = new TwoPCProtocol.GetControllerResp(prepReq.txId,prepReq.pId,rp);
        ms.sendAsync(address,TwoPCProtocol.GetControllerResp.class.getName(),s.encode(resp));
    }


    //TODO: MUDAR DE LOG PARA MAP
    private void putTwoPC1(Address address, byte[] m){
        System.out.println("PUT in keystore");
        TwoPCProtocol.ControllerPreparedReq prepReq = s.decode(m);
        int trans_id = prepReq.txId;
        if(log.actionAlreadyExists(trans_id, Phase.PREPARED) ){
            System.out.println("OO");
            TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
        }
        else if (log.actionAlreadyExists(trans_id, Phase.ROLLBACKED)){
            TwoPCProtocol.ControllerAbortResp p = new TwoPCProtocol.ControllerAbortResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerAbortResp.class.getName(),s.encode(p));
        }
        else if(!log.actionAlreadyExists(trans_id, Phase.PREPARED) && !log.actionAlreadyExists(trans_id, Phase.ROLLBACKED)) {
            System.out.print("You prepared for transaction " + trans_id + "?");
            String line = sc.nextLine();
            if (line != null && line.equals("yes")) {
                Map<Long, byte[]> keys =  prepReq.values;
                pendent_puts.put(trans_id, keys);
                log.write(trans_id,keys);
                log.write(trans_id, Phase.PREPARED.toString());
                TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId);
                ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
            } else {
                log.write(trans_id, Phase.ROLLBACKED.toString());
                TwoPCProtocol.ControllerAbortReq p = new TwoPCProtocol.ControllerAbortReq(trans_id,myId);
                ms.sendAsync(address, TwoPCProtocol.ControllerAbortReq.class.getName(),s.encode(p));
            }
        }
    }

    private void putTwoPC2(Address address, byte[] m) {
        TwoPCProtocol.ControllerCommitReq commitReq = s.decode(m);
        int trans_id = commitReq.txId;
        if (log.actionAlreadyExists(trans_id, Phase.COMMITED)){
            System.out.println("OO");
            TwoPCProtocol.ControllerCommitedResp p = new TwoPCProtocol.ControllerCommitedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerCommitedResp.class.getName(),s.encode(p));
        }
        else if(!log.actionAlreadyExists(trans_id, Phase.COMMITED)) {
            data.putAll(pendent_puts.get(trans_id));
            pendent_puts.remove(trans_id);
            log.write(trans_id, Phase.COMMITED.toString());
            System.out.println("Transaction " + trans_id + " commited!!");
            TwoPCProtocol.ControllerCommitedResp p = new TwoPCProtocol.ControllerCommitedResp(trans_id,myId);
            ms.sendAsync(address, TwoPCProtocol.ControllerCommitedResp.class.getName(),s.encode(p));
        }
    }

    public static void main(String[] args) {
        int id = Integer.parseInt(args[0]);
        new KeystoreSrv(id);
    }





}