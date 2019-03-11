package ch.digitalfondue.synckv;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class SyncKVStructuredTable<T> {

    private final SyncKVTable table;
    private final DataConverter<T> dataConverter;

    SyncKVStructuredTable(SyncKVTable table, DataConverterFrom<T> from, DataConverterTo<T> to) {
        this.table = table;
        this.dataConverter = new DataConverter<>(from, to);
    }

    public T get(String key) {
        byte[] res = table.get(key);
        return res == null ? null : dataConverter.from(res);
    }

    public void put(String key, T value) {
        table.put(key, value == null ? null : dataConverter.to(value));
    }

    public Set<String> keySet() {
        return table.keySet();
    }

    public Iterator<String> keys() {
        return table.keys();
    }

    public int count() {
        return table.count();
    }

    public Stream<Map.Entry<String, T>> stream() {
        return table.keySet().stream().map(key -> new MapEntry(key, get(key)));
    }

    private static class MapEntry<T> implements Map.Entry<String, T> {

        private String key;
        private T value;

        private MapEntry(String key, T value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public T getValue() {
            return value;
        }

        @Override
        public T setValue(T t) {
            throw new IllegalStateException("cannot set value in entry");
        }
    }


    class DataConverter<T> {

        private final DataConverterFrom<T> fromConverter;
        private final DataConverterTo<T> toConverter;

        DataConverter(DataConverterFrom<T> from, DataConverterTo<T> to) {
            this.fromConverter = from;
            this.toConverter = to;
        }

        T from(byte[] a) {
            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(a))) {
                return fromConverter.apply(dis);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        byte[] to(T a) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream daos = new DataOutputStream(baos);
            try {
                toConverter.apply(a, daos);
                return baos.toByteArray();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @FunctionalInterface
    public interface DataConverterFrom<T> {
        T apply(DataInputStream dis) throws IOException;
    }

    @FunctionalInterface
    public interface DataConverterTo<T> {
        void apply(T a, DataOutputStream daos) throws IOException;
    }
}
