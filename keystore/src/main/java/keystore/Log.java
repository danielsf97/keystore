package keystore;

import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

import java.util.List;

public class Log {

    public enum Phase {
        STARTED, PREPARED, COMMITED, ROLLBACKED
    }

    public static class LogEntry<T> {
        private int xid;
        private T data;
        private Phase phase;

        public LogEntry(int xid, T data) {
            this.xid=xid;
            this.phase = Phase.STARTED;
            this.data = data; //os participantes no coord e chaves nos participantes

        }

        public void setPhase(Phase phase){
            this.phase = phase;
        }

    }

    private Serializer s;
    private SegmentedJournal<Object> j;
    private SegmentedJournalWriter<Object> w;

    public Log() {
        this.s = Serializer.builder()
                .withTypes(Log.LogEntry.class)
                .build();

        this.j = SegmentedJournal.builder()
                .withName("log_coordenador")
                .withSerializer(s)
                .build();
        this.w = j.writer();
    }

    public void write(int transId, Phase phase) {
        w = j.writer();
        w.append(new Log.LogEntry(trans_id, interveniente, action));
        w.flush();
        //w.close();

    }


//    public static void main(String[] args) {
//        Serializer s = Serializer.builder()
//                .withTypes(LogEntry.class)
//                .build();
//
//        SegmentedJournal<Object> j = SegmentedJournal.builder()
//                .withName("exemplo")
//                .withSerializer(s)
//                .build();
//
//        int xid = 0;
//
//        SegmentedJournalReader<Object> r = j.openReader(0);
//        while(r.hasNext()) {
//            LogEntry e = (LogEntry) r.next().entry();
//            System.out.println(e.toString());
//            xid = e.xid;
//        }
//
//        SegmentedJournalWriter<Object> w = j.writer();
//        w.append(new LogEntry(xid+1, "hello world"));
//        w.flush();
//        w.close();
//    }
}
