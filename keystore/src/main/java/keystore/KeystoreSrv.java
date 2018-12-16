package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KeystoreSrv implements Keystore {


    private static final Address[] addresses = {
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };

    private Scanner sc = new Scanner(System.in);
    Map<Long, byte[]> data;
    private static ManagedMessagingService ms;
    private static Serializer s;
    private ExecutorService es;
    private Log log;
    private int myId;
    private Address coordinator;

    public KeystoreSrv(int id){


        this.myId = id;

        this.ms = NettyMessagingService.builder()
                .withAddress(addresses[myId])
                .build();


        //Recebe um pedido de prepared
        this.ms.registerHandler(TwoPCProtocol.ControllerPreparedReq.class.getName(), (o, m) ->{
            System.out.println("PUT in keystore");
            TwoPCProtocol.ControllerPreparedReq prepReq = s.decode(m);
            int trans_id = prepReq.txId;
            if(!log.actionAlreadyExists(trans_id, Log.Phase.PREPARED) && !log.actionAlreadyExists(trans_id, Log.Phase.ROLLBACKED)) {
                System.out.print("You prepared for transaction " + trans_id + "?");
                String line = sc.nextLine();
                if (line != null && line.equals("yes")) {
                    log.write(trans_id, Log.Phase.PREPARED);
                    TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId,TwoPCProtocol.Status.PREPARED_OK);
                    ms.sendAsync(o, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
                } else {
                    log.write(trans_id, Log.Phase.ROLLBACKED);
                    TwoPCProtocol.ControllerPreparedResp p = new TwoPCProtocol.ControllerPreparedResp(trans_id,myId,TwoPCProtocol.Status.ABORT);
                    ms.sendAsync(o, TwoPCProtocol.ControllerPreparedResp.class.getName(),s.encode(p));
                }
            }

        },es);

        this.ms.registerHandler(TwoPCProtocol.ControllerCommitReq.class.getName(), (o,m) ->{
            TwoPCProtocol.ControllerCommitReq commitReq = s.decode(m);
            int trans_id = commitReq.txId;
            if(!log.actionAlreadyExists(trans_id, Log.Phase.COMMITED)) {
                log.write(trans_id, Log.Phase.COMMITED);
                System.out.println("Transaction " + trans_id + " commited!!");
            }
        },es);

        this.s = KeystoreProtocol
                .newSerializer();

        this.es = Executors.newSingleThreadExecutor();

        this.log = new Log();

        data = new HashMap<>();
        //TODO: aqui ir buscar a informação ao Log

        ms.start();
    }



    @Override
    public CompletableFuture<Boolean> put(Map<Long, byte[]> values) throws Exception {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> keys) throws Exception {
        return CompletableFuture.completedFuture(new HashMap<>());
    }

    public static void main(String[] args) {
        int id = Integer.parseInt(args[0]);
        KeystoreSrv worker = new KeystoreSrv(id);
        System.out.println("HH");
    }

/*    private void sendMsg(Address address, String conteudo, int trans_id) {
        Msg msg = new Msg(myId, conteudo, trans_id);
        ms.sendAsync(address, TwoPCProtocol.ControllerPreparedResp.class.getName(), s.encode(msg));
    }*/



}