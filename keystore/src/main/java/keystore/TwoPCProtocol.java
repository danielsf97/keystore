package keystore;

import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.Map;

public class TwoPCProtocol {

    public static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        TwoPCProtocol.ControllerPreparedResp.class,
                        TwoPCProtocol.ControllerPreparedReq.class,
                        ControllerCommitReq.class,
                        ControllerCommitedResp.class,
                        GetControllerReq.class,
                        TwoPCProtocol.ControllerAbortReq.class,
                        ControllerReq.class,
                        TwoPCProtocol.ControllerReq.class,
                        TwoPCProtocol.ControllerAbortReq.class,
                        TwoPCProtocol.ControllerAbortResp.class
                        )
                .build();
    }





    public static class ControllerReq{
        public int txId;
        public int pId;
        public ControllerReq(int txId, int pId){
            this.txId = txId;
            this.pId = pId;
        }
    }

    public static class ControllerPreparedReq extends ControllerReq{
        public Map<Long,byte[]> values;
        public ControllerPreparedReq(int txId, int pId, Map<Long,byte[]> values){
            super(txId,pId);
            this.values = values;
        }
    }

    public static class ControllerCommitReq extends ControllerReq{
        public ControllerCommitReq(int txId, int pId){
            super(txId,pId);
        }
    }

    public static class ControllerAbortReq extends ControllerReq{
        public ControllerAbortReq(int txId, int pId){
            super(txId,pId);
        }
    }

    public static class ControllerResp{
        public int txId;
        public int pId;
        public ControllerResp(int txId, int pId){
            this.txId = txId;
            this.pId = pId;
        }
    }
    public static class ControllerPreparedResp extends ControllerResp{
        public ControllerPreparedResp(int txId, int pId){
            super(txId,pId);
        }
    }


    public static class ControllerCommitedResp extends ControllerResp{
        public ControllerCommitedResp(int txId, int pId){
            super(txId,pId);
        }
    }

    public static class ControllerAbortResp extends ControllerResp{
        public ControllerAbortResp(int txId, int pId){
            super(txId,pId);
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
