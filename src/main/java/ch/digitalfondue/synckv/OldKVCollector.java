package ch.digitalfondue.synckv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            Map<String, List<byte[]>> candidates = new HashMap<>();
            table.getKeysWithRawKey().forEach(k -> {
                if (!candidates.containsKey(k.getKey())) {
                    candidates.put(k.getKey(), new ArrayList<>());
                }
                candidates.get(k.getKey()).add(k.getValue());
            });
            candidates.forEach((k, v) -> {
                if (v.size() > 1) {
                    v.subList(0, v.size() - 1).forEach(keyToBeRemoved -> {
                        table.deleteRawKV(keyToBeRemoved);
                    });
                }
            });
        }
    }
}
