package ch.digitalfondue.synckv;

class OldKVCollector implements Runnable {

    private final SyncKV syncKV;

    OldKVCollector(SyncKV syncKV) {
        this.syncKV = syncKV;
    }

    @Override
    public void run() {
        if (syncKV.isCompactingDisabled()) {
            return;
        }

        for (String tableName : syncKV.getTableNames()) {
            SyncKVTable table = syncKV.getTable(tableName);
            table.collectOldKeys();
        }
    }
}
