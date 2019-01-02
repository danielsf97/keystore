package keystore;

import io.atomix.utils.serializer.Serializer;
import keystore.tpc.TwoPCProtocol;

import java.util.Collection;
import java.util.Map;

public class Server_KeystoreSrvProtocol extends TwoPCProtocol {

    public static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        TwoPCProtocol.ControllerReq.class,
                        TwoPCProtocol.ControllerPreparedReq.class,
                        TwoPCProtocol.ControllerCommitReq.class,
                        TwoPCProtocol.ControllerAbortReq.class,
                        TwoPCProtocol.ControllerResp.class,
                        TwoPCProtocol.ControllerPreparedResp.class,
                        TwoPCProtocol.ControllerCommittedResp.class,
                        TwoPCProtocol.ControllerAbortResp.class,
                        GetControllerReq.class,
                        GetControllerResp.class
                )
                .build();
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nos pedidos
     * get do servidor principal para os servidores de chaves
     *
     */
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

    /**
     * Representa o formato de serialização das mensagens utilizado nas respostas
     * aos pedidos get dos servidores de chaves para o servidor principal
     *
     */
    public static class GetControllerResp {
        public int txId;
        public int pId;
        public Map<Long, byte[]> values;
        public GetControllerResp(int txId, int pId, Map <Long, byte[]> values){
            this.txId = txId;
            this.pId = pId;
            this.values = values;
        }
    }

}
