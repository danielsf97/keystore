package tpc;

import io.atomix.utils.serializer.Serializer;

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

    static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        TwoPCProtocol.ControllerReq.class,
                        TwoPCProtocol.ControllerPreparedReq.class,
                        TwoPCProtocol.ControllerCommitReq.class,
                        TwoPCProtocol.ControllerAbortReq.class,
                        TwoPCProtocol.ControllerResp.class,
                        TwoPCProtocol.ControllerPreparedResp.class,
                        TwoPCProtocol.ControllerCommittedResp.class,
                        TwoPCProtocol.ControllerAbortResp.class
                        )
                .build();
    }

    /**
     * Representa o formato geral de serialização das mensagens correspondentes
     * a pedidos do coordenador aos participantes no protocolo 2PC
     *
     */
    static class ControllerReq {
        int txId;
        int pId;

        ControllerReq(int txId, int pId) {
            this.txId = txId;
            this.pId = pId;
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nos pedidos
     * de Prepared, do coordenador para os participantes
     *
     */
    static class ControllerPreparedReq <T> extends ControllerReq {
        T values;

        ControllerPreparedReq(int txId, int pId, T values) {
            super(txId, pId);
            this.values = values;
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nos pedidos
     * de Commit da transação, do coordenador para os participantes
     *
     */
    static class ControllerCommitReq extends ControllerReq {
        ControllerCommitReq(int txId, int pId) {
            super(txId, pId);
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nos pedidos
     * de abort da transação, do coordenador para os participantes
     *
     */
    static class ControllerAbortReq extends ControllerReq {
        ControllerAbortReq(int txId, int pId) {
            super(txId, pId);
        }
    }

    /**
     * Representa o formato geral de serialização das mensagens de resposta
     *  aos pedidos do coordenador para os  participantes no protocolo 2PC
     *
     */
    static class ControllerResp {
        int txId;
        int pId;
        ControllerResp(int txId, int pId) {
            this.txId = txId;
            this.pId = pId;
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nas respostas
     * aos pedidos de Prepared, dos participantes para o coordenador
     *
     */
    static class ControllerPreparedResp extends ControllerResp {
        ControllerPreparedResp(int txId, int pId) {
            super(txId, pId);
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nas respostas
     * aos pedidos de Commit da transação, dos participantes para o coordenador
     *
     */
    static class ControllerCommittedResp extends ControllerResp {
        ControllerCommittedResp(int txId, int pId) {
            super(txId, pId);
        }
    }

    /**
     * Representa o formato de serialização das mensagens utilizado nas respostas
     * aos pedidos de abort de transação, dos participantes para o coordenador
     *
     */
    static class ControllerAbortResp extends ControllerResp {
        ControllerAbortResp(int txId, int pId) {
            super(txId, pId);
        }
    }


}
