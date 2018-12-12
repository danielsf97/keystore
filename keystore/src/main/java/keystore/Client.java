package keystore;


import spullara.nio.channels.FutureServerSocketChannel;
import spullara.nio.channels.FutureSocketChannel;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Client {

    public static void main(String[] args) throws Exception {


        Keystore ks = new KeystoreCli(10000);

        ks.put(new HashMap<Long, byte[]>(){{
                    put(1000000L, "Chave 1".getBytes());
                    put(1000001L, "Chave 2".getBytes());
                    put(1000002L, "Chave 3".getBytes());
                    put(1000003L, "Chave 4".getBytes());
                    put(1000004L, "Chave 5".getBytes());
        }}).thenAccept((b) -> {
            if(b){
                System.out.println("Chaves Adicionadas");
            }else{
                System.out.println("Chaves não Adicionadas");
            }
        });

        ks.get(new ArrayList<Long>(){{
            add(1000002L);
            add(1000004L);
        }}).thenAccept(((res) -> {
            for( Map.Entry<Long, byte[]> entry : res.entrySet()){
                System.out.println("Key: " + entry.getKey() + ", Value:" + entry.getValue());
            }
        }));


    }
}
