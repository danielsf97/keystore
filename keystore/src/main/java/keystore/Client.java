package keystore;


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
     * @throws Exception    TODO:
     */
    public static void main(String[] args) throws Exception {


        Keystore ks = new KeystoreCli(12350);


        Map<Long,byte[]> chaves = new HashMap<>();
        chaves.put(1000000L, "Chave 1".getBytes());
        chaves.put(1000001L, "Chave 2".getBytes());
     //   chaves.put(1000002L, "Chave 3".getBytes());
        chaves.put(1000003L, "Chave 4".getBytes());
    //    chaves.put(1000004L, "Chave 5".getBytes());

        ks.put(chaves).thenAccept((b) -> {
            if(b){
                System.out.println("Chaves Adicionadas");
            }else{
                System.out.println("Chaves não Adicionadas");
            }
        });

        ks.put(chaves).thenAccept((b) -> {
            if(b){
                System.out.println("Chaves Adicionadas2");
            }else{
                System.out.println("Chaves não Adicionadas2");
            }
        });

        ks.put(chaves).thenAccept((b) -> {
            if(b){
                System.out.println("ks1:Chaves Adicionadas");
            }else{
                System.out.println("ks1:Chaves não Adicionadas");
            }
        });

        ks.put(chaves).thenAccept((b) -> {
            if(b){
                System.out.println("ks1:Chaves Adicionadas2");
            }else{
                System.out.println("ks1: Chaves não Adicionadas2");
            }
        });

        ks.put(chaves).thenAccept((b) -> {
            if(b){
                System.out.println("ks2: Chaves Adicionadas");
            }else{
                System.out.println("ks2: Chaves não Adicionadas");
            }
        });

        ks.put(chaves).thenAccept((b) -> {
            if(b){
                System.out.println("ks2: Chaves Adicionadas2");
            }else{
                System.out.println("ks2:  Chaves não Adicionadas2");
            }
        });

        Map<Long,byte[]> chaves1 = new HashMap<>();

        chaves1.put(1000003L, "Chave 1".getBytes());

        chaves1.put(1000001L, "Chave 2".getBytes());
        //   chaves.put(1000002L, "Chave 3".getBytes());
        chaves1.put(1000009L, "Chave 4".getBytes());

        chaves1.put(1000004L, "Chave 5".getBytes());


        ks.put(chaves1).thenAccept((b) -> {
            if(b) {
                System.out.println("Chaves Adicionadas3");
            }
            else {
                System.out.println("Chaves não Adicionadas3");
            }
        });

/*        ks1.put(chaves1).thenAccept((b) -> {
            if(b){
                System.out.println("ks1: Chaves Adicionadas3");
            }else{
                System.out.println("ks1: Chaves não Adicionadas3");
            }
        });

        ks2.put(chaves1).thenAccept((b) -> {
            if(b){
                System.out.println("ks2: Chaves Adicionadas3");
            }else{
                System.out.println("ks2: Chaves não Adicionadas3");
            }
        });*/

      /*  ks.put(chaves).thenAccept((b) -> {
            if(b){
                System.out.println("Chaves Adicionadas");
            }else{
                System.out.println("Chaves não Adicionadas");
            }
        });*/


       /* Collection<Long> keys = new ArrayList<>();
        keys.add(1000000L);
        keys.add(1000004L);
        ks.get(keys).thenAccept(((res) -> {
            System.out.println("Resultado de Get:");
            for (Map.Entry<Long, byte[]> entry : res.entrySet()) {
                System.out.println("Key: " + entry.getKey() + ", Value:" + new String(entry.getValue()));
            }
        }));*/


    }
}
