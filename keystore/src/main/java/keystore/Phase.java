package keystore;

public enum Phase {
    STARTED, PREPARED, COMMITED, ABORT, ROLLBACKED
}

/*
    private CompletableFuture<Void> check_response(CompletableFuture<Void> cp,  TwoPCProtocol.ControllerReq contReq, Address a, int rep,String type){
        System.out.println("Waiting for response. Remaning attempts:" + rep);
        if (rep > 0){
            try {
                cp.get(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                ms.sendAsync(a,
                        type,
                        sp.encode(contReq));
                System.out.println("HELLOoooooo");
                check_response(cp, contReq,a,--rep,type);
            }
        }
        else {
            if (!currentFuturesP1.get(contReq.txId).get(contReq.pId).isDone()){
                initAbort(contReq.txId);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> check_response(CompletableFuture<Void> cp,  TwoPCProtocol.ControllerReq contReq, Address a,String type){
        System.out.println("Waiting for response from:" + a.toString());
        try {
            cp.get(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            ms.sendAsync(a,
                    type,
                    sp.encode(contReq));
            System.out.println("HELLOoooooo");
            check_response(cp, contReq,a,type);
        }
        return CompletableFuture.completedFuture(null);
    }*/
