package tpc;

import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class TwoPCTransaction {



    private Integer id;
    private Collection<Integer> participants;
    private Map<Integer, Phase> participants_status;
    private Phase phase;
    private Integer client_txId;
    private Address address;
    private ReentrantLock lock;


    public TwoPCTransaction(Integer id, Integer client_txId, Address address){
        this.id = id;
        this.client_txId = client_txId;
        this.phase = Phase.STARTED;
        this.address = address;
        this.participants_status = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    TwoPCTransaction(SimpleTwoPCTransaction tx, Phase phase){
        this.id = tx.id;
        this.client_txId = tx.clientId;
        this.address = Address.from(tx.clientAddress);
        this.participants = tx.participantsToT.keySet();
        this.participants_status = new HashMap<>();
        this.phase = phase;
        this.lock = new ReentrantLock();
        for(Integer p : participants){
            participants_status.put(p, phase);
        }
    }

    public int getId(){
        return id;
    }

    public Address getAddress(){
        return address;
    }

    public int get_client_txId(){
        return client_txId;
    }

    Phase getPhase(){
        return phase;
    }

    Collection<Integer> getParticipants() {
        return participants;
    }

    public Phase getParticipantStatus(int pId){
        return participants_status.get(pId);
    }



    public void setPhase(Phase phase){
        this.phase = phase;
    }

    public void setParticipants(Collection<Integer> participants){
        this.participants = participants;
        for(Integer p : participants){
            participants_status.put(p, Phase.STARTED);
        }
    }

    public void setParticipantStatus(Integer participant, Phase resp){
        System.out.println("CHANGING STATUS");
        participants_status.put(participant, resp);
    }


    public boolean checkParticipantsPhases(Phase phase){
        boolean status = true;

        for (Phase s: participants_status.values()){
            if (s != phase )
                status= false;
        }

        return status;
    }



    public void lock(){
        this.lock.lock();
    }

    public void unlock(){
        this.lock.unlock();
    }


}
