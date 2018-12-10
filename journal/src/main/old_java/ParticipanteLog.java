import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

public class ParticipanteLog {

    private Serializer s;
    private SegmentedJournal<Object> j;
    private int read_pointer = 0;

    public static class LogEntry {
        public int xid;
        public String data;

        public LogEntry() {}
        public LogEntry(int xid, String data) {
            this.xid=xid;
            this.data=data;
        }

        public String toString() {
            return "xid="+xid+" "+data;
        }
    }

    public ParticipanteLog(int id){
        this.s = Serializer.builder()
                .withTypes(TestLog.LogEntry.class)
                .build();

        this.j = SegmentedJournal.builder()
                .withName("log_participante_" + id)
                .withSerializer(s)
                .build();
    }

    public void read(){
        SegmentedJournalReader<Object> r = j.openReader(read_pointer);
        while(r.hasNext()) {
            TestLog.LogEntry e = (TestLog.LogEntry) r.next().entry();
            System.out.println(e.toString());
            read_pointer = e.xid;
        }
    }

    public void write(int trans_id, String action){
        SegmentedJournalWriter<Object> w = j.writer();
        w.append(new TestLog.LogEntry(trans_id, action));
        w.flush();
        w.close();
    }
}
