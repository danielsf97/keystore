package keystore;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import tpc.Log;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Serv {


    public ManagedMessagingService ms;
    public Log<Object> log;
    public ExecutorService es;



    public Serv(Address address)
    {
        es = Executors.newSingleThreadExecutor();

        ms = NettyMessagingService.builder()
                .withAddress(address)
                .build();
    }


}
