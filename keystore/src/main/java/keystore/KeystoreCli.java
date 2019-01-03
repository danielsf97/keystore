package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;


/**
 * Representa uma keystore distribuída, permitindo ao cliente
 * obter e color pares chave-valor na mesma.
 *
 */
public class KeystoreCli implements Keystore {


    // ***************************************************************************
    // Variáveis
    // ***************************************************************************

    private final ManagedMessagingService ms;
    private final Address srv;
    private final Serializer s;
    private final Map<Integer, CompletableFuture<Boolean>> put_requests;
    private final Map<Integer,CompletableFuture<Map<Long, byte[]>>> get_requests;
    private final ExecutorService es;
    private int i;


    // **************************************************************************
    // Construtores
    // **************************************************************************

    /**
     * Construtor parametrizado para o KeystoreCli.
     *
     * @param port          Porta onde está a correr o servidor.
     * @throws Exception    TODO:
     */
    KeystoreCli(int port) throws Exception {
        i = 0;
        es = Executors.newSingleThreadExecutor();
        srv = Address.from("localhost", port);
        ms = NettyMessagingService.builder()
                .withAddress(Address.from(10000))
                .build();
        s = KeystoreProtocol.newSerializer();

        put_requests = new HashMap<>();
        get_requests = new HashMap<>();

        ms.registerHandler(KeystoreProtocol.PutResp.class.getName(), (o, m) -> {
            KeystoreProtocol.PutResp putResp = s.decode(m);
            System.out.println("RESPONSE");
            if (put_requests.containsKey(putResp.txId)) {
                put_requests.remove(putResp.txId).complete(putResp.state);
            }
        }, es);

        ms.registerHandler(KeystoreProtocol.GetResp.class.getName(), (o, m) -> {
            KeystoreProtocol.GetResp getResp = s.decode(m);

            get_requests.remove(getResp.txId).complete(getResp.values);

        }, es);

        ms.start().get();
    }


    // **************************************************************************
    // Métodos públicos
    // **************************************************************************

    /**
     * Permite colocar um conjunto de pares chave-valor na keystore
     * distribuída.
     *
     * @param values        Map cujas entradas correspondem aos pares chave-valor.
     * @return              Booleano indicativo do sucesso (true) ou insucesso
     *                      (false) da operação.
     * @throws Exception    TODO:
     */
    @Override
    public CompletableFuture<Boolean> put(Map<Long, byte[]> values) {

        KeystoreProtocol.PutReq putReq = new KeystoreProtocol.PutReq(values, i);
        ms.sendAsync(srv,"put", s.encode(putReq));

        CompletableFuture<Boolean> cp = new CompletableFuture<>();
        put_requests.put(i, cp);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(()->{
            if (put_requests.containsKey(i)){
                ms.sendAsync(srv,"put", s.encode(putReq));
            }
            else{
                scheduler.shutdown();
            }
        }, 30, 10 , TimeUnit.SECONDS);

        i++;

        return cp;
    }


    /**
     * Permite obter os valores existentes na keystore correspondentes a um conjunto
     * de chaves dado.
     *
     * @param keys          Conjunto de chaves para as quais se pretende obter os
     *                      respetivos valores.
     * @return              Map cujas entradas correspondem aos pares chave-valor
     *                      das chaves providenciadas.
     * @throws Exception    TODO:
     */
    @Override
    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> keys) {
        KeystoreProtocol.GetReq getReq = new KeystoreProtocol.GetReq(keys, i);
        ms.sendAsync(srv,"get", s.encode(getReq));

        CompletableFuture<Map<Long, byte[]>> cp = new CompletableFuture<>();
        get_requests.put(i, cp);

        i++;

        return cp;
    }
}
