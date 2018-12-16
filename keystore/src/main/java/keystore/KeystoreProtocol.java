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
        Map<Long,byte[]> values;
        public PutReq(Map<Long,byte[]> values) {
            this.values = values;
        }
    }

    public static class PutResp {
        boolean state;
        public PutResp(boolean state) {
            this.state = state;
        }
    }

    public static class GetReq {
        Collection<Long> keys;
        public GetReq(Collection<Long> keys) {
            this.keys = keys;
        }
    }

    public static class GetResp {
        Map<Long, byte[]> values;
        public GetResp(Map<Long, byte[]> values) {
            this.values = values;
        }
    }

}
