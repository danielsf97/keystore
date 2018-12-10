import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.*;
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
                if(readyForCommit && !log.actionAlreadyExists(trans_id, "COMMITED")){
                    System.out.println("Transaction " + trans_id + " commited!!");
                    log.write(trans_id, my_id, "COMMITED");
                    broadcast(trans_id, "commit");
                }
                break;
            case "not ok":
                if(!log.actionAlreadyExists(trans_id, "ROLLBACK")){
                    System.out.println("Transaction " + trans_id + " rollbacked!!");
                    log.write(trans_id, my_id, "ROLLBACK");
                    broadcast(trans_id, "rollback");
                }
                break;
        }
    }

    private void broadcast(int trans_id, String conteudo) {
        Msg msg = new Msg(my_id, conteudo, trans_id);

        for(Address address: addresses){
            ms.sendAsync(address, "exemplo", s.encode(msg));
        }
    }

    private List notPreparedAdresses(int trans_id) {

        List list = new ArrayList(Arrays.asList(addresses));

        if(conc_transactions.containsKey(trans_id)){
            TreeSet<Integer> trans_intrs = conc_transactions.get(trans_id);
            for(Integer idx : trans_intrs)
                list.remove(addresses[idx]);
        }
        return list;
    }

    private boolean checkTransaction(int trans_id) {

        if(conc_transactions.containsKey(trans_id)){
            TreeSet<Integer> trans_intrs = conc_transactions.get(trans_id);
            if(trans_intrs.size() == addresses.length) return true;
        }
        return false;
    }

    private void addPrepared(int trans_id, int from) {
        if(!conc_transactions.containsKey(trans_id)){
            TreeSet<Integer> new_trans = new TreeSet<>();
            new_trans.add(from);
            conc_transactions.put(trans_id, new_trans);
        }else{
            TreeSet<Integer> trans_intrs = conc_transactions.get(trans_id);
            trans_intrs.add(from);
            conc_transactions.put(trans_id,trans_intrs);
        }
    }

    private void sendMsg(Address address, String conteudo, int trans_id) {
        Msg msg = new Msg(my_id, conteudo, trans_id);
        ms.sendAsync(address, "exemplo", s.encode(msg));
    }

    private void new_transaction(int trans_id) throws Exception{
        int n_attempts = 5;
        boolean completed = false;
        boolean rollbacked = false;
        for(int i = 0; i < n_attempts && !completed && !rollbacked; i++ ){
            sendTo(trans_id, "prepared",notPreparedAdresses(trans_id));
            Thread.sleep(5000);
            if(checkTransaction(trans_id)) completed = true;
            else if(log.actionAlreadyExists(trans_id,"ROLLBACK")) rollbacked = true;
        }
        if(!completed && !rollbacked){
            System.out.println("Transaction " + trans_id + " rollbacked!!");
            log.write(trans_id, my_id, "ROLLBACK");
            broadcast(trans_id, "rollback");
        }
    }

    private void sendTo(int trans_id, String conteudo, List notPreparedAdresses) {

        Msg msg = new Msg(my_id, conteudo, trans_id);

        for(Object address: notPreparedAdresses){
            ms.sendAsync((Address) address, "exemplo", s.encode(msg));
        }

    }

    public static void main(String [] args) throws Exception{
        int nr_trans = 15;

        Coordenador coordenador = new Coordenador();

        for(int i = 0; i < nr_trans; i++ ){
            System.in.read();
            coordenador.new_transaction(i);
        }
    }
}
