package keystore;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Transaction {



    public enum Phase {
        STARTED, PREPARED, COMMITED, ROLLBACKED
    }

    private Integer id;
    private Collection<Integer> participants;
    private Map<Integer,TwoPCProtocol.Status> participants_status;
    private Phase phase;


    public Transaction(Integer id){

        this.id = id;
        this.phase = Phase.STARTED;
        this.participants_status = new HashMap<>();

    }


    public void setPhase(Phase phase){
        this.phase = phase;
    }

    public Phase getPhase (){
        return phase;
    }

    public void setParticipants(Collection <Integer> participants){
        this.participants = participants;
        for(Integer p : participants){
            participants_status.put(p,TwoPCProtocol.Status.PREPARED_REQ);
        }
    }

    public void setParticipant_resp( Integer participant, TwoPCProtocol.Status resp){
        if (phase == Phase.STARTED){
            participants_status.put(participant, resp);
        }
        else if (phase == Phase.PREPARED){
            participants_status.put(participant,resp);
        }
    }

    public int getId(){
        return id;
    }

    public Collection<Integer> getParticipants() {
        return participants;
    }

    public TwoPCProtocol.Status getParticipantStatus(int pId){
        return participants_status.get(pId);
    }

    public boolean check_prepared() {
        boolean prepared_status = true;

        for (TwoPCProtocol.Status s : participants_status.values()){
            if (s != TwoPCProtocol.Status.PREPARED_OK)
                prepared_status = false;
        }
        return prepared_status;
    }

    public boolean check_commit(){
        boolean prepared_commit = true;
        System.out.println(participants_status.size());
        for (TwoPCProtocol.Status s: participants_status.values()){
            System.out.println(s.toString());
            if (s != TwoPCProtocol.Status.COMMITED)
                prepared_commit = false;
        }
        System.out.println(prepared_commit);

        return prepared_commit;
    }

}
