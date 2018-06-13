package ch.digitalfondue.synckv;

class SynchronizationHandler implements Runnable {

    private final SyncKV syncKV;
    private final RpcFacade rpcFacade;

    SynchronizationHandler(SyncKV syncKV, RpcFacade rpcFacade) {
        this.syncKV = syncKV;
        this.rpcFacade = rpcFacade;
    }

    @Override
    public void run() {
        if(syncKV.isLeader()) {
            processRequestForSync();
            rpcFacade.requestForSyncPayload(syncKV.getAddress());
        }
    }

    private void processRequestForSync() {
    }
}
