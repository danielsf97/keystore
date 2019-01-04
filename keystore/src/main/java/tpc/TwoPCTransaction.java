package tpc;

import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Representa uma transação no contexto do algoritmo de Two-Phase Commit.
 *
 */
public class TwoPCTransaction {

    // ***************************************************************************
    // Variáveis
    // ***************************************************************************

    private Integer id;
    private Collection<Integer> participants;
    private Map<Integer, Phase> participantsStatus;
    private Phase phase;
    private Integer clientTxId;
    private Address address;
    private ReentrantLock lock;


    // ***************************************************************************
    // Construtores
    // ***************************************************************************

    /**
     * Construtor de uma transação no contexto do algoritmo de Two-Phase Commit.
     *
     * @param id            Identificaro único da transação.
     * @param clientTxId    Identificador da transação no contexto do cliente.
     * @param address       Endereço do clinte.
     */
    public TwoPCTransaction(Integer id, Integer clientTxId, Address address) {
        this.id = id;
        this.clientTxId = clientTxId;
        this.phase = Phase.STARTED;
        this.address = address;
        this.participantsStatus = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    /**
     * Construtor de uma transação no contexto do algoritmo de Two-Phase Commit.
     *
     * @param tx        Formato da mesma transação quando inserida no log do coordenador.
     * @param phase     Fase que inicializa esta transação.
     */
    TwoPCTransaction(SimpleTwoPCTransaction tx, Phase phase) {
        this.id = tx.id;
        this.clientTxId = tx.clientId;
        this.address = Address.from(tx.clientAddress);
        this.participants = tx.participantsToT.keySet();
        this.participantsStatus = new HashMap<>();
        this.phase = phase;
        this.lock = new ReentrantLock();
        for(Integer p : participants) {
            participantsStatus.put(p, phase);
        }
    }

    // ***************************************************************************
    // Getters e Setters
    // ***************************************************************************

    /**
     * Obtém o identificador de uma transação.
     *
     * @return o identificador de uma transação.
     */
    public int getId() {
        return id;
    }

    public Address getAddress() {
        return address;
    }

    public int getClientTxId() {
        return clientTxId;
    }


    /**
     * Obtém a fase em que se encontra uma transação.
     *
     * @return fase da transação.
     */
    Phase getPhase() {
        return phase;
    }


    /**
     * Obtém a coleção de participantes da transação.
     *
     * @return Identificadores dos participantes da transação.
     */
    Collection<Integer> getParticipants() {
        return participants;
    }


    /**
     *
     *
     * @param pId
     * @return
     */
    public Phase getParticipantStatus(int pId) {
        return participantsStatus.get(pId);
    }


    /**
     * Altera a fase de uma transação.
     *
     * @param phase Fase para a qual alterar a transação.
     */
    public void setPhase(Phase phase) {
        this.phase = phase;
    }


    /**
     * Estabelece os participantes de uma transação.
     *
     * @param participants Participantes de uma transação.
     */
    public void setParticipants(Collection<Integer> participants) {
        this.participants = participants;

        for(Integer p : participants) {
            participantsStatus.put(p, Phase.STARTED);
        }
    }

    /**
     * Altera o estado(fase) do particiante para com a transação.
     *
     * @param participant       Identificador do participante.
     * @param resp              Fase a atribuir ao participante.
     */
    public void setParticipantStatus(Integer participant, Phase resp) {
        participantsStatus.put(participant, resp);
    }


    // ***************************************************************************
    // Métodos públicos
    // ***************************************************************************


    /**
     * Verifica se as fases dos participantes são todas iguais a uma determinada
     * fase.
     *
     * @param phase Fase para a qual se permite saber se os participantes estão
     *              nela
     * @return      Boolean indicativo de os participantes estarem todos na fase
     *              (true) ou não estarem.
     */
    public boolean checkParticipantsPhases(Phase phase) {
        boolean status = true;

        for(Phase s: participantsStatus.values()) {
            if(s != phase)
                status= false;
        }

        return status;
    }

    // ***************************************************************************
    // Lock e unlock
    // ***************************************************************************

    /**
     * Obtém o lock de uma transação.
     *
     */
    public void lock() {
        this.lock.lock();
    }


    /**
     * Liberta o lock de uma transação.
     */
    public void unlock() {
        this.lock.unlock();
    }
}
