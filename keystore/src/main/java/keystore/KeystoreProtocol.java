package keystore;

import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.Map;


/**
 * Define as mensagens utilizadas na comunicação dos clientes com o servidor
 * (pedidos e respostas PUT/GET) e permite criar um serializer para as classes definidas
 *
 */
class KeystoreProtocol {

    /**
     * Cria um Serializador para as mensagens entre Cliente e Servidor
     *
     * @return              Serializer das mensagens Cliente-Servidor
     */
    static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        KeystoreProtocol.PutReq.class,
                        KeystoreProtocol.PutResp.class,
                        KeystoreProtocol.GetReq.class,
                        KeystoreProtocol.GetResp.class)
                .build();
    }

    /**
     * Representa o formato de serialização das mensagens referentes aos pedidos
     * put do cliente para o servidor
     *
     */
    static class PutReq {
        int txId;
        Map<Long,byte[]> values;

        PutReq(Map<Long, byte[]> values, int txId) {
            this.values = values;
            this.txId = txId;
        }
    }

    /**
     * Representa o formato de serialização das mensagens referentes às respostas
     * dos pedidos put do servidor para o cliente
     *
     */
    static class PutResp {
        int txId;
        boolean state;

        PutResp(boolean state, int txId) {
            this.state = state;
            this.txId = txId;
        }
    }

    /**
     * Representa o formato de serialização das mensagens referentes aos pedidos
     * get do cliente para o servidor
     *
     */
    static class GetReq {
        int txId;
        Collection<Long> keys;

        GetReq(Collection<Long> keys, int txId) {
            this.keys = keys;
            this.txId = txId;
        }
    }

    /**
     * Representa o formato de serialização das mensagens referentes às respostas
     * dos pedidos get do servidor para o cliente
     *
     */
    static class GetResp {
        int txId;
        Map<Long, byte[]> values;

        GetResp(Map<Long, byte[]> values, int txId) {
            this.values = values;
            this.txId = txId;
        }
    }
}
