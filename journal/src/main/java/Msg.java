public class Msg<T> {

    private int from;
    private T msg;
    private int trans_id;

    public Msg(int from, T msg, int trans_id) {
        this.from = from;
        this.msg = msg;
        this.trans_id = trans_id;
    }

    public int getFrom() {
        return from;
    }

    public T getMsg() {
        return msg;
    }

    public int getTrans_id() { return trans_id; }
}