package keystore;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * Representa uma keystore distribuída, permitindo ao cliente
 * obter e color pares chave-valor na mesma.
 *
 */
public interface Keystore {

    /**
     * Permite colocar um conjunto de pares chave-valor na keystore
     * distribuída.
     *
     * @param values        Map cujas entradas correspondem aos pares chave-valor.
     * @return              Booleano indicativo do sucesso (true) ou insucesso (false) da operação.
     */
    CompletableFuture<Boolean> put(Map<Long, byte[]> values);


    /**
     * Permite obter os valores existentes na keystore correspondentes a um conjunto
     * de chaves dado.
     *
     * @param keys          Conjunto de chaves para as quais se pretende obter os respetivos valores.
     * @return              Map cujas entradas correspondem aos pares chave-valor das chaves providenciadas.
     */
    CompletableFuture<Map <Long, byte[]>> get(Collection<Long> keys);
}
