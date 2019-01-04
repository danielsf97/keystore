package tpc;

import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class TwoPCTransaction {



    private Integer id;
    private Collection<Integer> participants;
    private Map<Integer, Phase> participantsStatus;
    private Phase phase;
    private Integer clientTxId;
    private Address address;
    private ReentrantLock lock;


    public TwoPCTransaction(Integer id, Integer clientTxId, Address address) {
        this.id = id;
        this.clientTxId = clientTxId;
        this.phase = Phase.STARTED;
        this.address = address;
        this.participantsStatus = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    TwoPCTransaction(SimpleTwoPCTransaction tx, Phase phase) {
        this.id = tx.id;
        this.clientTxId = tx.clientId;
        this.address = Address.from(tx.clientAddress);
        this.participants = tx.participantsToT.keySet();
        this.participantsStatus = new HashMap<>();
        this.phase = phase;
        this.lock = new ReentrantLock();
        for(Integer p : participants) {
            participantsStatus.put(p, phase);
        }
    }

    public int getId() {
        return id;
    }

    public Address getAddress() {
        return address;
    }

    public int getClientTxId() {
        return clientTxId;
    }

    Phase getPhase() {
        return phase;
    }

    Collection<Integer> getParticipants() {
        return participants;
    }

    public Phase getParticipantStatus(int pId) {
        return participantsStatus.get(pId);
    }

    public void setPhase(Phase phase) {
        this.phase = phase;
    }

    public void setParticipants(Collection<Integer> participants) {
        this.participants = participants;

        for(Integer p : participants) {
            participantsStatus.put(p, Phase.STARTED);
        }
    }

    public void setParticipantStatus(Integer participant, Phase resp) {
        participantsStatus.put(participant, resp);
    }

    public boolean checkParticipantsPhases(Phase phase) {
        boolean status = true;

        for (Phase s: participantsStatus.values()) {
            if (s != phase )
                status= false;
        }

        return status;
    }

    public void lock() {
        this.lock.lock();
    }

    public void unlock() {
        this.lock.unlock();
    }
}
