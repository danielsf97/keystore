import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

import java.util.HashMap;
import java.util.TreeSet;

public class CoordenadorLog {

    private Serializer s;
    private SegmentedJournal<Object> j;
    private SegmentedJournalWriter<Object> w;
    private int read_pointer = 0;

    public static class LogEntry {
        public int trans_id;
        public int interveniente;
        public String data;

        public LogEntry() {}
        public LogEntry(int trans_id, int interveniente, String data) {
            this.trans_id = trans_id;
            this.interveniente = interveniente;
            this.data = data;
        }

        public int getTrans_id() {
            return trans_id;
        }

        public int getInterveniente() {
            return interveniente;
        }

        public String getData() {
            return data;
        }

    }

    public HashMap<Integer,TreeSet<Integer>> getTransactions() {
        HashMap<Integer, TreeSet<Integer>> res = new HashMap<>();

        SegmentedJournalReader<Object> r = j.openReader(0);
        int trans = 0;
        while(r.hasNext()) {
            CoordenadorLog.LogEntry e = (CoordenadorLog.LogEntry) r.next().entry();
            trans = e.getTrans_id();
            if(!res.containsKey(trans)){
                if(e.getInterveniente() != -1){
                    TreeSet<Integer> new_trans = new TreeSet<>();
                    new_trans.add(e.getInterveniente());
                    res.put(trans, new_trans);
                }
            }else{
                if(e.getInterveniente() == -1){ //é um marcador (commited ou prepared)
                    res.remove(trans);
                }else{
                    TreeSet<Integer> trans_intrs = res.get(trans);
                    trans_intrs.add(e.getInterveniente());
                    res.put(trans,trans_intrs);
                }
            }
        }
        return res;
    }

    public CoordenadorLog(){
        this.s = Serializer.builder()
                .withTypes(CoordenadorLog.LogEntry.class)
                .build();

        this.j = SegmentedJournal.builder()
                .withName("log_coordenador")
                .withSerializer(s)
                .build();
        this.w = j.writer();
    }

    public void read(){
        SegmentedJournalReader<Object> r = j.openReader(read_pointer);
        while(r.hasNext()) {
            TestLog.LogEntry e = (TestLog.LogEntry) r.next().entry();
            System.out.println(e.toString());
            read_pointer = e.xid;
        }
    }

    public void write(int trans_id, int interveniente, String action){
        w = j.writer();
        w.append(new CoordenadorLog.LogEntry(trans_id, interveniente, action));
        w.flush();
        //w.close();
    }

    public boolean actionAlreadyExists(int trans_id, String action) {
        boolean exists = false;
        SegmentedJournalReader<Object> r = j.openReader(0);
        System.out.println("Verificar existência de ação: " + trans_id + " -> " + action);
        while(r.hasNext() && !exists) {
            CoordenadorLog.LogEntry e = (CoordenadorLog.LogEntry) r.next().entry();
            System.out.println(e.trans_id + " -> " + e.data);
            if(e.trans_id == trans_id && action.equals(e.data)) exists = true;
        }
        System.out.println("Returned " + exists);
        return exists;
    }
}
