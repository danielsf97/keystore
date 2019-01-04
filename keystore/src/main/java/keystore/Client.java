package keystore;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
     * @throws Exception    Exceçãp
     */
    public static void main(String[] args) throws Exception {


        Keystore ks = new KeystoreCli(12350);

        //Está a colocar no servidor com id 1
        Map<Long, byte[]> keys = new HashMap<>();
        keys.put(1000000L, "Chave 1".getBytes()); // id 1
   //     keys.put(1000001L, "Chave 2".getBytes()); // id 2
     //   keys.put(1000002L, "Chave 3".getBytes()); // id 0
        keys.put(1000003L, "Chave 4".getBytes());  // id 1
   //     keys.put(1000004L, "Chave 5".getBytes());   // id 2


        ks.put(keys).thenAccept((b) -> {
            if(b) {
                System.out.println("Chaves Adicionadas1");
            }
            else {
                System.out.println("Chaves não Adicionadas1");
            }
        });

        ks.put(keys).thenAccept((b) -> {
            if(b) {
                System.out.println("Chaves Adicionadas2");
            }
            else {
                System.out.println("Chaves não Adicionadas2");
            }
        });

        ks.put(keys).thenAccept((b) -> {
            if(b) {
                System.out.println("Chaves Adicionadas3");
            }
            else {
                System.out.println("Chaves não Adicionadas3");
            }
        });



        Collection<Long> keys2 = new ArrayList<>();
        keys2.add(1000000L);
        keys2.add(1000003L);
        ks.get(keys2).thenAccept(((res) -> {
            System.out.println("Resultado de Get:");
            for (Map.Entry<Long, byte[]> entry : res.entrySet()) {
                System.out.println("Key: " + entry.getKey() + ", Value:" + new String(entry.getValue()));
            }
        }));


    }
}
