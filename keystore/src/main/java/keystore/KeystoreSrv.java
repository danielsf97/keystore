package keystore;

import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import keystore.tpc.Participant;

import java.util.*;

public class KeystoreSrv extends Participant {


    private static final Address[] addresses = new Address[]{
            Address.from("localhost:12345"),
            Address.from("localhost:12346"),
            Address.from("localhost:12347")
    };


    private static Serializer s;

    private KeystoreSrv(int id){
        super(addresses[id],"KeyStoreSrv" + id, id);

        s  = Server_KeystoreSrvProtocol
                .newSerializer();

        ms.registerHandler(Server_KeystoreSrvProtocol.GetControllerReq.class.getName(), this::get, es);


        System.out.println("Size: " + data.size());

        ms.start();
    }



    private void get(Address address, byte[] m) {
        System.out.println("GET in keystore");
        Server_KeystoreSrvProtocol.GetControllerReq prepReq = s.decode(m);
        Collection<Long> keys = prepReq.keys;
        Map<Long,byte[]> rp = new HashMap<>();
        for(Long e : keys){

            synchronized (data) {
                if (data.containsKey(e))
                    rp.put(e,(byte[]) data.get(e));
            }
        }
        Server_KeystoreSrvProtocol.GetControllerResp resp = new Server_KeystoreSrvProtocol.GetControllerResp(prepReq.txId,prepReq.pId,rp);
        ms.sendAsync(address,Server_KeystoreSrvProtocol.GetControllerResp.class.getName(),s.encode(resp));
    }




    public static void main(String[] args) {
        int id = Integer.parseInt(args[0]);
        new KeystoreSrv(id);
    }





}