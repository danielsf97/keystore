package keystore;

import java.util.List;

public class Transaction {

    public enum Phase {
        STARTED, PREPARED, COMMITED, ROLLBACKED
    }

    private List<Integer> participants;
    private Phase phase;

    public Transaction(List<Integer> participants){
        this.participants = participants;
        this.phase = Phase.STARTED;
    }

    public void setPhase(Phase phase){
        this.phase = phase;
    }

    public List<Integer> getParticipants() {
        return participants;
    }
}
