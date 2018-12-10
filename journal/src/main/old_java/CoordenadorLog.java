import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.utils.serializer.Serializer;

public class CoordenadorLog {

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

    public CoordenadorLog(){
        Serializer s = Serializer.builder()
                .withTypes(TestLog.LogEntry.class)
                .build();

        SegmentedJournal<Object> j = SegmentedJournal.builder()
                .withName("exemplo")
                .withSerializer(s)
                .build();
    }
}
