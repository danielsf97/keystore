package keystore;

import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.Map;

public class KeystoreProtocol {
    public static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        KeystoreProtocol.PutReq.class,
                        KeystoreProtocol.PutResp.class,
                        KeystoreProtocol.GetReq.class,
                        KeystoreProtocol.GetResp.class)
                .build();
    }


    public static class PutReq {
        int txId;
        Map<Long,byte[]> values;
        public PutReq(Map<Long,byte[]> values, int txId) {
            this.values = values;
            this.txId = txId;
        }
    }

    public static class PutResp {
        int txId;
        boolean state;
        public PutResp(boolean state, int txId) {
            this.state = state;this.txId=txId;
        }
    }

    public static class GetReq {
        int txId;
        Collection<Long> keys;
        public GetReq(Collection<Long> keys, int txId) {
            this.keys = keys;
            this.txId = txId;
        }
    }

    public static class GetResp {
        int txId;
        Map<Long, byte[]> values;
        public GetResp(Map<Long, byte[]> values, int txId) {
            this.values = values;
            this.txId = txId;
        }
    }

}
