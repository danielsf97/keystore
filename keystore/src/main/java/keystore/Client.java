package keystore;


import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Client {

    public static void main(String[] args) throws Exception {


        Keystore ks = new KeystoreCli(12350);

        ExecutorService es = Executors.newSingleThreadExecutor();

        Map<Long,byte[]> chaves = new HashMap<>();
        chaves.put(1000000L, "Chave 1".getBytes());
  //      chaves.put(1000001L, "Chave 2".getBytes());
     //   chaves.put(1000002L, "Chave 3".getBytes());
        chaves.put(1000003L, "Chave 4".getBytes());
     //   chaves.put(1000004L, "Chave 5".getBytes());

        ks.put(chaves).thenAccept((b) -> {
            if(b){
                System.out.println("Chaves Adicionadas");
            }else{
                System.out.println("Chaves não Adicionadas");
            }
        });


/*        ks.get(new ArrayList<Long>(){{
            add(1000002L);
            add(1000004L);
        }}).thenAccept(((res) -> {
            for( Map.Entry<Long, byte[]> entry : res.entrySet()){
                System.out.println("Key: " + entry.getKey() + ", Value:" + entry.getValue());
            }
        }));*/


    }
}
