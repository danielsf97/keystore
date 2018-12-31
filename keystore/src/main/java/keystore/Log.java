package keystore;

import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;


/**
 * Representa um log.
 *
 * @param <T> TIpo de entrada no log
 */
public class Log<T> {

    /**
     * Representa uma entrada no log.
     *
     * @param <T>   Tipo de entrada no log.
     */
    public static class LogEntry<T> {


        // **********************************************************************
        // Variáveis
        // **********************************************************************

        private int trans_id;
        private T action;


        // **********************************************************************
        // Construtores
        // **********************************************************************

        /**
         * Construtor parametrizado de uma entrada no log.
         *
         * @param trans_id      ID da entrada.
         * @param action        Conteúdo da entrada.
         */
        public LogEntry(int trans_id, T action) {
            this.trans_id = trans_id;
            this.action = action;

        }

/*       public void setPhase(Phase phase){
            this.phase = phase;
        }*/

        // **********************************************************************
        // Getters e setters
        // **********************************************************************

        /**
         * Retorna o ID de uma entrada.
         *
         * @return  o ID de uma entrada.
         */
        public int getTrans_id() {
            return trans_id;
        }

        /**
         * Retorna o conteúdo de uma entrada.
         *
         * @return o conteúdo de uma entrada.
         */
        public T getAction() {
            return action;
        }

        // **********************************************************************
        // Métodos públicos
        // **********************************************************************


        /**
         * Constrói a representação textual de uma entrada no log.
         *
         * @return a representação textual de uma entrada no log.
         */
        public String toString() {
            return "xid=" + trans_id + " " + action;
        }

    }


    // **************************************************************************
    // Variáveis
    // **************************************************************************

    private Serializer s;
    private SegmentedJournal<Object> j;
    private SegmentedJournalWriter<Object> w;


    // **************************************************************************
    // Construtores
    // **************************************************************************

    /**
     * Construtor parametrizado do log.
     *
     * @param name Nome do log.
     */
    public Log(String name) {
        this.s = Serializer.builder()
                .withTypes(Log.LogEntry.class)
                .withTypes(SimpleTransaction.class)
                .build();

        this.j = SegmentedJournal.builder()
                .withName(name)
                .withSerializer(s)
                .build();

        this.w = j.writer();
    }


    // **************************************************************************
    // Métodos públicos
    // **************************************************************************

    /**
     * Retorna o último indíce escrito no log.
     *
     * @return o último indíce escrito no log.
     */
    public long last_index() {
        System.out.println(w.getLastIndex());
        return w.getLastIndex();
    }


    /**
     * Escreve no log.
     *
     * @param transId   Identificador da entrada.
     * @param action    Conteúdo da entrada no log.
     */
    public void write(int transId, T action) {
        w = j.writer();
        w.append(new Log.LogEntry<Object>(transId, action));
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


    /**
     * Lê uma entrada no log localizada num dado índice.
     *
     * @param index     Índice onde se pretende ler a entrada.
     * @return          Entrada localizada no indíce dado.
     */
    public Log.LogEntry read(int index) {
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
            System.out.println(r.getCurrentIndex() +  e.trans_id + " -> " + e.action);
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
