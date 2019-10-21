package ch.digitalfondue.synckv;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.BiFunction;
import java.util.function.Function;

class TableSyncExecutor {

    private ScheduledThreadPoolExecutor scheduledExecutor;
    private Function<String, SyncKVTable> tableSupplier;

    public TableSyncExecutor(ScheduledThreadPoolExecutor scheduledExecutor, Function<String, SyncKVTable> tableSupplier) {
        this.scheduledExecutor = scheduledExecutor;
        this.tableSupplier = tableSupplier;
    }

    void scheduleFullTableSync(String tableName, BiFunction<List<KV>, Boolean, CompletableFuture<Boolean>> sendRaw) {
        scheduledExecutor.execute(() -> {
            SyncKVTable table = tableSupplier.apply(tableName);
            List<KV> res = new ArrayList<>();
            Iterator<byte[]> it = table.rawKeys();
            while (it.hasNext()) {
                byte[] k = it.next();
                byte[] v = table.getRawKV(k);
                if (v != null) {
                    res.add(new KV(k, v));
                }
                if (res.size() > 250) {
                    sendRaw.apply(res, false).join();
                    res = new ArrayList<>();
                }
            }
            sendRaw.apply(res, true).join();
        });
    }

    void schedulePartialTableSync(String tableName, List<TreeSync.ExportLeaf> remote, BiFunction<List<KV>, Boolean, CompletableFuture<Boolean>> sendRaw) {
        scheduledExecutor.execute(() -> {
            SyncKVTable localTable = tableSupplier.apply(tableName);
            TreeSync localTreeSync = localTable.getTreeSync();
            localTreeSync.removeMatchingLeafs(remote);
            List<KV> res = new ArrayList<>();
            Iterator<byte[]> it = localTable.rawKeys();
            while(it.hasNext()) {
                byte[] key = it.next();
                if (localTreeSync.isInExistingBucket(key)) {
                    byte[] value = localTable.getRawKV(key);
                    if (value != null) {
                        res.add(new KV(key, localTable.getRawKV(key)));
                    }
                }
                if (res.size() > 250) {
                    sendRaw.apply(res, false).join();
                    res = new ArrayList<>();
                }
            }
            sendRaw.apply(res, true).join();
        });
    }
}
