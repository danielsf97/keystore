package keystore;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * O programa Client é usado para teste do sistema distribuído de
 * armazenamento de pares chave-valor, sendo responsável
 * por usar as operações providenciadas pelo servidor de get e put
 * das mesmas.
 *
 */
public class Client {


    /**
     * Usa as operações de get e put de pares chaves-valor, após
     * conexão ao servidor.
     *
     * @param args          Um array de strings contendo os argumentos
     *                      da linha de comandos do programa.
     * @throws Exception    TODO:
     */
    public static void main(String[] args) throws Exception {


        Keystore ks = new KeystoreCli(12350);

        ExecutorService es = Executors.newSingleThreadExecutor();

        Map<Long,byte[]> chaves = new HashMap<>();
        chaves.put(1000000L, "Chave 1".getBytes());
        chaves.put(1000001L, "Chave 2".getBytes());
     //   chaves.put(1000002L, "Chave 3".getBytes());
        chaves.put(1000003L, "Chave 4".getBytes());
        chaves.put(1000004L, "Chave 5".getBytes());

        ks.put(chaves).thenAccept((b) -> {
            if(b){
                System.out.println("Chaves Adicionadas");
            }else{
                System.out.println("Chaves não Adicionadas");
            }
        }).get();

      /*  ks.put(chaves).thenAccept((b) -> {
            if(b){
                System.out.println("Chaves Adicionadas");
            }else{
                System.out.println("Chaves não Adicionadas");
            }
        });*/


        Collection<Long> keys = new ArrayList<>();
        keys.add(1000000L);
        keys.add(1000004L);
        ks.get(keys).thenAccept(((res) -> {
            for( Map.Entry<Long, byte[]> entry : res.entrySet()){
                System.out.println("Key: " + entry.getKey() + ", Value:" + new String(entry.getValue()));
            }
        }));

    }
}
