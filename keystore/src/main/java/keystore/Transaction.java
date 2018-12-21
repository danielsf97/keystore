package keystore;

import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class Transaction {



    private Integer id;
    private Collection<Integer> participants;
    private Map<Integer,Phase> participants_status;
    private Map<Long,byte[]> keys;
    private Phase phase;
    private Integer client_txId;
    private Address address;




    Transaction(Integer id, Integer client_txId,Address address){

        this.id = id;
        this.client_txId = client_txId;
        this.phase = Phase.STARTED;
        this.address = address;
        this.participants_status = new HashMap<>();
        keys = new HashMap<>();

    }

    Transaction(SimpleTransaction tx, Phase phase){
        this.id = tx.id;
        this.client_txId = tx.clientId;
        this.address = Address.from(tx.clientAddress);
        this.participants = tx.participantsToKeys.keySet();
        this.participants_status = new HashMap<>();
        this.phase = phase;
        for(Integer p : participants){
            participants_status.put(p, phase);
        }
    }

    Address getAddress(){
        return address;
    }

    int get_client_txId(){
        return client_txId;
    }

    void setPhase(Phase phase){
        this.phase = phase;
    }

    Phase getPhase(){
        return phase;
    }

    void setParticipants(Collection<Integer> participants){
        this.participants = participants;
        for(Integer p : participants){
            participants_status.put(p, Phase.STARTED);
        }
    }



    void setParticipant_resp(Integer participant, Phase resp){
        System.out.println("CHANGING STATUS");
        if (phase == Phase.STARTED){
            participants_status.put(participant, resp);
        }
        else if (phase == Phase.PREPARED){
            participants_status.put(participant,resp);
        }
        else if (phase == Phase.ABORT){
            participants_status.put(participant,resp);
        }
    }

    int getId(){
        return id;
    }

    Collection<Integer> getParticipants() {
        return participants;
    }

    Phase getParticipantStatus(int pId){
        return participants_status.get(pId);
    }


    boolean check_phase(Phase phase){
        boolean status = true;

        for (Phase s: participants_status.values()){
            if (s != phase )
                status= false;
        }

        return status;
    }


    public void setKeys(Map<Long,byte[]> chaves){
        System.out.println("A colocar chaves");
        keys.putAll(chaves);
    }

    public Map<Long,byte[]> getKeys (){
        return keys;
    }


}
