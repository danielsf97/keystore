import com.sun.xml.internal.ws.wsdl.writer.document.Part;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Participante {

    private int id;

    private static final Address[] addresses = {
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };

    private Scanner sc = new Scanner(System.in);
    private ManagedMessagingService ms;
    private ExecutorService es;
    private Serializer s;
    private int my_id;
    private ParticipanteLog log;

    public Participante(int my_id) {
        this.my_id = my_id;
        this.ms = NettyMessagingService.builder()
                .withAddress(addresses[my_id])
                .build();

        this.s = new SerializerBuilder()
                .addType(Msg.class)
                .build();

        this.es = Executors.newSingleThreadExecutor();

        this.log = new ParticipanteLog(my_id);

        ms.registerHandler("exemplo", (o, m) -> {
            Msg msg = s.decode(m);

            processaMsg(o, msg);

        }, es);

        ms.start();
    }

    private void processaMsg(Address address, Msg msg){
        int from  = msg.getFrom();

        if(from == -1){
            int trans_id = msg.getTrans_id();

            switch((String) msg.getMsg()){
                case "prepared":
                    System.out.print("You prepared for transaction " + trans_id + "?");
                    String line = sc.nextLine();
                    if(line != null && line.equals("yes")){
                        log.write(trans_id, "PREPARED");
                        sendMsg(address, "ok", msg.getTrans_id());
                    }
                    else{
                        log.write(trans_id, "ROLLBACK");
                        sendMsg(address, "not ok", msg.getTrans_id());
                    }
                    break;
                case "commit":
                    log.write(trans_id, "COMMITED");
                    System.out.println("Transaction " + msg.getTrans_id() + " commited!!");
                    break;
            }
        }
    }

    private void sendMsg(Address address, String conteudo, int trans_id) {
        Msg msg = new Msg(my_id, conteudo, trans_id);
        ms.sendAsync(address, "exemplo", s.encode(msg));
    }

    public static void main(String [] args){

        int my_id = Integer.parseInt(args[0]);
        Participante participante = new Participante(my_id);

    }

}
