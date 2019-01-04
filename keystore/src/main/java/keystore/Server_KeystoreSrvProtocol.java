package keystore;

import io.atomix.utils.serializer.Serializer;
import tpc.TwoPCProtocol;

import java.util.Collection;
import java.util.Map;

public class Server_KeystoreSrvProtocol extends TwoPCProtocol {

    public static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
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
    static class GetControllerReq {
        int txId;
        int pId;
        Collection<Long> keys;
        GetControllerReq(int txId, int pId, Collection<Long> keys){
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
    static class GetControllerResp {
        int txId;
        int pId;
        Map<Long, byte[]> values;
        GetControllerResp(int txId, int pId, Map <Long, byte[]> values){
            this.txId = txId;
            this.pId = pId;
            this.values = values;
        }
    }

}
