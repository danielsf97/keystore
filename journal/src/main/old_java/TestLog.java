import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

public class TestLog {
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

    public static void main(String[] args) {
        Serializer s = Serializer.builder()
                .withTypes(LogEntry.class)
                .build();

        SegmentedJournal<Object> j = SegmentedJournal.builder()
                .withName("exemplo")
                .withSerializer(s)
                .build();

        int xid = 0;

        SegmentedJournalReader<Object> r = j.openReader(0);
        while(r.hasNext()) {
            LogEntry e = (LogEntry) r.next().entry();
            System.out.println(e.toString());
            xid = e.xid;
        }

        SegmentedJournalWriter<Object> w = j.writer();
        w.append(new LogEntry(xid+1, "hello world"));
        w.flush();
        w.close();
    }
}
