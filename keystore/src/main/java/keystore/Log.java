package keystore;

import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

import java.util.List;

public class Log<T> {

    public enum Phase {
        STARTED, PREPARED, COMMITED, ROLLBACKED
    }

    public static class LogEntry<T> {
        private int trans_id;
        private T action;

        public LogEntry(int trans_id, T action) {
            this.trans_id=trans_id;
            this.action = action; //os participantes no coord e chaves nos participantes

        }

/*       public void setPhase(Phase phase){
            this.phase = phase;
        }*/
        public String toString() {
            return "xid="+trans_id+" "+action;
        }

        public int getTrans_id(){
            return trans_id;
        }

        public T getAction(){
            return action;
        }

    }

    private Serializer s;
    private SegmentedJournal<Object> j;
    private SegmentedJournalWriter<Object> w;

    public Log(String name) {
        this.s = Serializer.builder()
                .withTypes(Log.LogEntry.class)
                .build();

        this.j = SegmentedJournal.builder()
                .withName(name)
                .withSerializer(s)
                .build();

        this.w = j.writer();


    }

    public void write(int transId, T action) {
        w = j.writer();
        w.append(new Log.LogEntry(transId, action));
        w.flush();
      //  w.close();

    }
    public void read(){
        SegmentedJournalReader<Object> r = j.openReader(0);
        while(r.hasNext()) {
            Log.LogEntry e = (Log.LogEntry) r.next().entry();
            System.out.println(e.toString());
        }
    }

    public Log.LogEntry read(int index){
        SegmentedJournalReader<Object> r = j.openReader(index);
        if (r.hasNext()) return (Log.LogEntry) r.next().entry();
        return null;
    }




    public boolean actionAlreadyExists(int trans_id, Phase ac) {
        boolean exists = false;
        SegmentedJournalReader<Object> r = j.openReader(0);
        String action = ac.toString();
        System.out.println("Verificar existência de ação: " + trans_id + " -> " + action);
        while(r.hasNext() && !exists) {
            Log.LogEntry e = (Log.LogEntry) r.next().entry();
            System.out.println(e.trans_id + " -> " + e.action);
            if(e.trans_id == trans_id && action.equals(e.action)) exists = true;
        }
        System.out.println("Returned " + exists);
        return exists;
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
