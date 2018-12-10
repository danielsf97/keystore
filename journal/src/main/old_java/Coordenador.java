import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.HashMap;
import java.util.Scanner;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Coordenador {

    private static final Address[] addresses = {
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };

    private final Address my_address = Address.from("localhost:12350");

    private final int my_id = -1;

    private Scanner sc = new Scanner(System.in);
    private ManagedMessagingService ms;
    private ExecutorService es;
    private Serializer s;
    private CoordenadorLog log;
    private HashMap<Integer, TreeSet<Integer>> conc_transactions;

    public Coordenador() {
        this.ms = NettyMessagingService.builder()
                .withAddress(my_address)
                .build();

        this.s = new SerializerBuilder()
                .addType(Msg.class)
                .build();

        this.es = Executors.newSingleThreadExecutor();

        this.log = new CoordenadorLog();

        this.conc_transactions = this.log.getTransactions();

        ms.registerHandler("exemplo", (o, m) -> {
            Msg msg = s.decode(m);

            processaMsg(o, msg);

        }, es);

        ms.start();
    }

    private void processaMsg(Address address, Msg msg){
        int from  = msg.getFrom();

        int trans_id = msg.getTrans_id();

        switch((String) msg.getMsg()){
            case "ok":
                addPrepared(trans_id, msg.getFrom());
                boolean readyForCommit = checkTransaction(trans_id);
                if(readyForCommit){
                    System.out.println("Transaction " + trans_id + " commited!!");
                    log.write(trans_id, "COMMITED");
                    broadcast(trans_id, "commit");
                }
                break;
            case "not ok":
                System.out.println("Transaction " + trans_id + " rollbacked!!");
                log.write(trans_id, "ROLLBACK");
                broadcast(trans_id, "rollback");
                break;
        }
    }

    private void addPrepared(int trans_id, int from) {
        if(!conc_transactions.containsKey())
    }

    private void sendMsg(Address address, String conteudo, int trans_id) {
        Msg msg = new Msg(my_id, conteudo, trans_id);
        ms.sendAsync(address, "exemplo", s.encode(msg));
    }

    public static void main(String [] args){

        int my_id = Integer.parseInt(args[0]);
        Coordenador coordenador = new Coordenador();

    }
}
