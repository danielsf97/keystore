import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

public class ParticipanteLog {

    private Serializer s;
    private SegmentedJournal<Object> j;
    private int read_pointer = 0;
    private SegmentedJournalWriter<Object> w;

    public static class LogEntry {
        public int trans_id;
        public String action;

        public LogEntry() {}
        public LogEntry(int trans_id, String action) {
            this.trans_id=trans_id;
            this.action=action;
        }

    }

    public ParticipanteLog(int id){
        this.s = Serializer.builder()
                .withTypes(ParticipanteLog.LogEntry.class)
                .build();

        this.j = SegmentedJournal.builder()
                .withName("log_participante_" + id)
                .withSerializer(s)
                .build();
        this.w = j.writer();

    }

    public void read(){
        SegmentedJournalReader<Object> r = j.openReader(0);
        while(r.hasNext()) {
            ParticipanteLog.LogEntry e = (ParticipanteLog.LogEntry) r.next().entry();
        }
    }

    public void write(int trans_id, String action){
        //SegmentedJournalWriter<Object> w = j.writer();
        //if(actionAlreadyExists(trans_id, action)) return;
        System.out.println("Write: " + trans_id + " -> " + action);
        w.append(new ParticipanteLog.LogEntry(trans_id, action));
        w.flush();
        System.out.println("Existe? " + actionAlreadyExists(trans_id, action));

        //w.close();
    }

    public boolean actionAlreadyExists(int trans_id, String action) {
        boolean exists = false;
        SegmentedJournalReader<Object> r = j.openReader(0);
        System.out.println("Verificar existência de ação: " + trans_id + " -> " + action);
        while(r.hasNext() && !exists) {
            ParticipanteLog.LogEntry e = (ParticipanteLog.LogEntry) r.next().entry();
            System.out.println(e.trans_id + " -> " + e.action);
            if(e.trans_id == trans_id && action.equals(e.action)) exists = true;
        }
        System.out.println("Returned " + exists);
        return exists;
    }
}
