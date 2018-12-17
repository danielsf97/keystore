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
    private Log log;
    private int myId;

    private Map<Integer,Map<Long, byte[]>> pendent_puts;

    private Map<Integer,Collection<Long>> pendent_gets;

    private KeystoreSrv(int id){


        this.myId = id;

        ms = NettyMessagingService.builder()
                .withAddress(addresses[myId])
                .build();
        s  = TwoPCProtocol
                .newSerializer();

        ExecutorService es = Executors.newSingleThreadExecutor();

        this.log = new Log();

        this.pendent_puts = new HashMap<>();
        this.pendent_gets = new HashMap<>();

        data = new HashMap<>();

        //Recebe um pedido de prepared
        ms.registerHandler(TwoPCProtocol.PutControllerPreparedReq.class.getName(), this::putTwoPC1, es);

        ms.registerHandler(TwoPCProtocol.PutControllerCommitReq.class.getName(), this::putTwoPC2, es);

        ms.registerHandler(TwoPCProtocol.GetControllerReq.class.getName(), this::get, es);

        //TODO: aqui ir buscar a informação ao Log

        ms.start();
    }

    private void get(Address address, byte[] m) {
        System.out.println("PUT in keystore");
        TwoPCProtocol.GetControllerReq prepReq = s.decode(m);
        int trans_id = prepReq.txId;
        Collection<Long> keys = prepReq.keys;
        Map<Long,byte[]> rp = new HashMap<>();
        for(Long e : keys){
            if (data.containsKey(e))
                rp.put(e,data.get(e));
        }
        TwoPCProtocol.GetControllerResp resp = new TwoPCProtocol.GetControllerResp(prepReq.txId,prepReq.pId,rp);
        ms.sendAsync(address,TwoPCProtocol.GetControllerResp.class.getName(),s.encode(resp));
    }


    private void putTwoPC1(Address address, byte[] m){
        System.out.println("PUT in keystore");
        TwoPCProtocol.PutControllerPreparedReq prepReq = s.decode(m);
        int trans_id = prepReq.txId;
     //   if(!log.actionAlreadyExists(trans_id, Log.Phase.PREPARED) && !log.actionAlreadyExists(trans_id, Log.Phase.ROLLBACKED)) {
        if(!log.actionAlreadyExists(trans_id, Log.Phase.PREPARED) && !log.actionAlreadyExists(trans_id, Log.Phase.ROLLBACKED)) {
            System.out.print("You prepared for transaction " + trans_id + "?");
            String line = sc.nextLine();
            if (line != null && line.equals("yes")) {
                Map<Long, byte[]> keys = prepReq.values;
                pendent_puts.put(trans_id, keys);
                log.write(trans_id, Log.Phase.PREPARED);
                TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId,TwoPCProtocol.Status.PREPARED_OK);
                System.out.print("Sending ok");
                ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
            } else {
                log.write(trans_id, Log.Phase.ROLLBACKED);
                TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId,TwoPCProtocol.Status.ABORT);
                ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
            }
        }
    }

    private void putTwoPC2(Address address, byte[] m) {
        TwoPCProtocol.PutControllerCommitReq commitReq = s.decode(m);
        int trans_id = commitReq.txId;
        if(!log.actionAlreadyExists(trans_id, Log.Phase.COMMITED)) {
            for (Map.Entry<Long,byte[]> e : pendent_puts.get(trans_id).entrySet())
                   data.put(e.getKey(),e.getValue());
            pendent_puts.remove(trans_id);
            log.write(trans_id, Log.Phase.COMMITED);
            System.out.println("Transaction " + trans_id + " commited!!");
            TwoPCProtocol.ControllerCommitResp p = new TwoPCProtocol.ControllerCommitResp(trans_id,myId,TwoPCProtocol.Status.COMMITED);
            ms.sendAsync(address,TwoPCProtocol.ControllerCommitResp.class.getName(),s.encode(p));
        }
    }

    public static void main(String[] args) {
        int id = Integer.parseInt(args[0]);
        KeystoreSrv worker = new KeystoreSrv(id);
    }

/*    private void sendMsg(Address address, String conteudo, int trans_id) {
        Msg msg = new Msg(myId, conteudo, trans_id);
        ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(), s.encode(msg));
    }*/



}