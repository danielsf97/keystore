package keystore;

import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.Map;

public class TwoPCProtocol {

    public static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        TwoPCProtocol.PutControllerPreparedReq.class,
                        TwoPCProtocol.ControllerPreparedResp.class,
                        PutControllerCommitReq.class,
                        TwoPCProtocol.ControllerCommitResp.class,
                        TwoPCProtocol.Status.class,
                        GetControllerReq.class,
                        TwoPCProtocol.Abort.class,
                        PutControllerReq.class
                        )
                .build();
    }

    public enum Status {
        PREPARED_REQ, PREPARED_OK, COMMITED, ABORT
    }


    public static class Abort{
        public int txId;
        public Abort(int txId){
            this.txId = txId;
        }
    }


    public static class PutControllerReq{
        public int txId;
        public int pId;
        public PutControllerReq(int txId, int pId){
            this.txId = txId;
            this.pId = pId;
        }
    }

    public static class PutControllerPreparedReq extends PutControllerReq{
        public Map<Long, byte[]> values;
        public PutControllerPreparedReq(int txId, int pId, Map<Long, byte[]> values){
            super(txId,pId);
            this.values = values;
        }
    }

    public static class PutControllerCommitReq extends PutControllerReq{
        public PutControllerCommitReq(int txId, int pId){
            super(txId,pId);
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


    public static class GetControllerReq {
        public int txId;
        public int pId;
        public Collection<Long> keys;
        public GetControllerReq(int txId, int pId, Collection<Long> keys){
            this.txId = txId;
            this.pId = pId;
            this.keys = keys;
        }
    }


    public static class GetControllerResp {
        public int txId;
        public int pId;
        public Map <Long, byte[]> values;
        public GetControllerResp(int txId, int pId, Map <Long, byte[]> values){
            this.txId = txId;
            this.pId = pId;
            this.values = values;
        }
    }


}
