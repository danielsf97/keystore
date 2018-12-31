package keystore;

import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.Map;

/**
 * Define as mensagens utilizadas no protocolo Two-Phase Commit,
 * entre Coordenador e Participantes das transações
 *
 */
public class TwoPCProtocol {

    /**
     * Cria um Serializador para as mensagens do protocolo 2PC
     *
     * @return              Serializer das mensagens do protocolo 2PC
     */
    public static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        TwoPCProtocol.ControllerPreparedResp.class,
                        TwoPCProtocol.ControllerPreparedReq.class,
                        ControllerCommitReq.class,
                        ControllerCommitedResp.class,
                        GetControllerReq.class,
                        GetControllerResp.class,
                        TwoPCProtocol.ControllerAbortReq.class,
                        ControllerReq.class,
                        TwoPCProtocol.ControllerReq.class,
                        TwoPCProtocol.ControllerAbortReq.class,
                        TwoPCProtocol.ControllerAbortResp.class
                        )
                .build();
    }

    /**
     * Representa o formato geral de serialização das mensagens correspondentes
     * a pedidos do coordenador aos participantes no protocolo 2PC
     *
     */
    public static class ControllerReq{
        public int txId;
        public int pId;
        public ControllerReq(int txId, int pId){
            this.txId = txId;
            this.pId = pId;
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nos pedidos
     * de Prepared, do coordenador para os participantes
     *
     */
    public static class ControllerPreparedReq extends ControllerReq{
        public Map<Long,byte[]> values;
        public ControllerPreparedReq(int txId, int pId, Map<Long,byte[]> values){
            super(txId,pId);
            this.values = values;
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nos pedidos
     * de Commit da transação, do coordenador para os participantes
     *
     */
    public static class ControllerCommitReq extends ControllerReq{
        public ControllerCommitReq(int txId, int pId){
            super(txId,pId);
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nos pedidos
     * de abort da transação, do coordenador para os participantes
     *
     */
    public static class ControllerAbortReq extends ControllerReq{
        public ControllerAbortReq(int txId, int pId){
            super(txId,pId);
        }
    }

    /**
     * Representa o formato geral de serialização das mensagens de resposta
     *  aos pedidos do coordenador para os  participantes no protocolo 2PC
     *
     */
    public static class ControllerResp{
        public int txId;
        public int pId;
        public ControllerResp(int txId, int pId){
            this.txId = txId;
            this.pId = pId;
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nas respostas
     * aos pedidos de Prepared, dos participantes para o coordenador
     *
     */
    public static class ControllerPreparedResp extends ControllerResp{
        public ControllerPreparedResp(int txId, int pId){
            super(txId,pId);
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nas respostas
     * aos pedidos de Commit da transação, dos participantes para o coordenador
     *
     */
    public static class ControllerCommitedResp extends ControllerResp{
        public ControllerCommitedResp(int txId, int pId){
            super(txId,pId);
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nas respostas
     * aos pedidos de abort de transação, dos participantes para o coordenador
     *
     */
    public static class ControllerAbortResp extends ControllerResp{
        public ControllerAbortResp(int txId, int pId){
            super(txId,pId);
        }
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
        public Map <Long, byte[]> values;
        public GetControllerResp(int txId, int pId, Map <Long, byte[]> values){
            this.txId = txId;
            this.pId = pId;
            this.values = values;
        }
    }


}
