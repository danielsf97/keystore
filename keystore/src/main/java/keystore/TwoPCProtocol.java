package keystore;

import io.atomix.utils.serializer.Serializer;

import java.util.Map;

public class TwoPCProtocol {

    public static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        TwoPCProtocol.ControllerPreparedReq.class,
                        TwoPCProtocol.ControllerPreparedResp.class,
                        TwoPCProtocol.ControllerCommitReq.class,
                        TwoPCProtocol.ControllerCommitResp.class)
                .build();
    }

    public enum Status {
        PREPARED_REQ, PREPARED_OK, COMMITED, ABORT
    }

    public static class ControllerPreparedReq{
        public int txId;
        public int pId;
        public Map<Long, byte[]> values;
        public ControllerPreparedReq(int txId, int pId, Map<Long, byte[]> values){
            this.txId = txId;
            this.pId = pId;
            this.values = values;
        }
    }

    public static class ControllerPreparedResp{
        public int txId;
        public int pId;
        public Status resp;
        public ControllerPreparedResp(int txId, int pId, Status resp){
            this.txId = txId;
            this.pId = pId;
            this.resp = resp;
        }
    }

    public static class ControllerCommitReq{
        public int txId;
        public int pId;
        public ControllerCommitReq(int txId, int pId){
            this.txId = txId;
            this.pId = pId;
        }
    }

    public static class ControllerCommitResp{
        public int txId;
        public int pId;
        public Status resp;
        public ControllerCommitResp(int txId, int pId, Status resp){
            this.txId = txId;
            this.pId = pId;
            this.resp = resp;
        }
    }
}
