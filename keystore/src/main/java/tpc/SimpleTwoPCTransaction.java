package tpc;

import io.atomix.utils.net.Address;

import java.util.Map;

/**
 * Define o formato de representação da informação de uma transação
 * a inserir no log do Servidor/Coordenador
 *
 */
class SimpleTwoPCTransaction<T> {
    Integer id;
    Integer clientId;
    Integer clientAddress;
    Map<Integer, T> participantsToT;

    /**
     * Construtor parametrizado para o SimpleTransaction.
     *
     * @param id                    Identificador único da transação no Servidor.
     * @param clientId              Identificador único da transação no Cliente.
     * @param clientAddress         Endereço do cliente
     * @param participantsToKeys    Mapa de associação dos servidores particiantes e chaves a armazenar
     */
    SimpleTwoPCTransaction(Integer id, Integer clientId, Address clientAddress, Map<Integer, T> participantsToT){
        this.id = id;
        this.clientId = clientId;
        this.clientAddress = clientAddress.port();
        this.participantsToT = participantsToT;
    }

}
