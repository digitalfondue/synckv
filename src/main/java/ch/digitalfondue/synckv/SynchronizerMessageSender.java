package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import org.h2.mvstore.MVStore;
import org.jgroups.JChannel;

import java.io.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class SynchronizerMessageSender implements Runnable {

    private final MVStore store;
    private final JChannel channel;
    private final ConcurrentHashMap<String, CountingBloomFilter> bloomFilters;

    public SynchronizerMessageSender(MVStore store, JChannel channel, ConcurrentHashMap<String, CountingBloomFilter> bloomFilters) {
        this.store = store;
        this.channel = channel;
        this.bloomFilters = bloomFilters;
    }

    @Override
    public void run() {
        List<SyncKVMessage.TableMetadata> toSync = store.getMapNames().stream().filter(SyncKV.IS_VALID_PUBLIC_TABLE_NAME)
                .map(name -> new SyncKVMessage.TableMetadata(name, store.openMap(name).size(), getSerializedBloomFilter(name)))
                .collect(Collectors.toList());

        try {
            channel.send(null, new SyncKVMessage.SyncPayload(toSync));
        } catch (Exception e) {
            //silently ignore the issue :°)
        }
    }

    private byte[] getSerializedBloomFilter(String name) {

        CountingBloomFilter cbf = bloomFilters.get(name);
        if(cbf == null) {
            return null;
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                cbf.write(new DataOutputStream(baos));
            } catch (IOException e) {
                //silently ignore the issue :°)
            }
            return baos.toByteArray();
        }
    }
}
