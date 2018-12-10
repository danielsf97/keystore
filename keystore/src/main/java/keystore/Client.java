package keystore;


import spullara.nio.channels.FutureServerSocketChannel;
import spullara.nio.channels.FutureSocketChannel;

import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public class Client {

/*    public CompletableFuture<Void> loop(FutureServerSocketChannel ss) {
        return ss.accept()
                .thenCompose((s)->{
                    System.out.println("Ligado");
                    Client c = new Client(s, this);
                    synchronized (clients) {
                        clients.add(c);
                    }
                    return CompletableFuture.allOf(c.rw(), loop(ss));
                });
    }*/

    public static void main(String[] args) throws Exception {

        Scanner scanner = new Scanner(System.in);

        int id = scanner.nextInt();
        // Serialização
        scanner.nextLine();

        //Por um Login e Register???



        Keystore ks = new KeystoreCli(10000 + id);


        CompletableFuture<Map<Long,byte[]>> oi = ks.get(new HashSet<>());




    }
}
