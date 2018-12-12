package keystore;

import io.atomix.utils.serializer.Serializer;

import java.util.Map;

public class TwoFCProtocol {

    public static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        TwoFCProtocol.ControllerPreparedReq.class,
                        TwoFCProtocol.ControllerPreparedResp.class)
                .build();
    }

    public static class ControllerPreparedReq{
        public int txId;
        public Map<Long, byte[]> values;
        public ControllerPreparedReq(int txId, Map<Long, byte[]> values){
            this.txId = txId;
            this.values = values;
        }
    }

    public static class ControllerPreparedResp{
        public int txId;
        public ControllerPreparedResp(int txId){
            this.txId = txId;
        }
    }
}
