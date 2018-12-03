package keystore;

import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.Map;

public class KeystoreProtocol {
    public static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        KeystoreProtocol.PutReq.class, KeystoreProtocol.PutRep.class,
                        KeystoreProtocol.GetReq.class, KeystoreProtocol.GetRep.class)
                .build();
    }


    public static class PutReq {
        Map<Long,byte[]> values;
        public PutReq(Map<Long,byte[]> values) {
            this.values = values;
        }
    }
    public static class PutRep {
        boolean state;
        public PutRep(boolean state) {
            this.state = state;
        }
    }

    public static class GetReq {
        Collection<Long> keys;
        public GetReq(Collection<Long> keys) {
            this.keys = keys;
        }
    }
    public static class GetRep {
        Map<Long, byte[]> values;
        public GetRep(Map<Long, byte[]> values) {
            this.values = values;
        }
    }


}
