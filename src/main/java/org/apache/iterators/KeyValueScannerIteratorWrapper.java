package org.apache.iterators;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.KeyValueHeap;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KeyValueScannerIteratorWrapper implements SortedKeyValueIterator<Key, Value> {

    private final List<? extends KeyValueScanner> scanners;
    private final Store store;
    private KeyValueHeap heap;
    private KeyValue loadedVK;

    public KeyValueScannerIteratorWrapper(List<? extends KeyValueScanner> scanners, Store store) {
        this.scanners = scanners;
        this.store = store;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        next();
    }

    @Override
    public boolean hasTop() {
        return loadedVK != null;
    }

    @Override
    public void next() throws IOException {
        if (heap == null) {
            heap = new KeyValueHeap(scanners, store.getComparator());
        }
        loadedVK = heap.next();
        System.out.println(loadedVK);
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        for (KeyValueScanner scanner : scanners) {
            scanner.seek(new KeyValue(range.getStartKey().getRow().getBytes(), range.getStartKey().getColumnFamily().getBytes(), range.getStartKey().getColumnQualifier().getBytes()));
        }
        heap = new KeyValueHeap(scanners, store.getComparator());
        next();
    }

    @Override
    public Key getTopKey() {
        return new Key(loadedVK.getRow(), loadedVK.getFamily(), loadedVK.getQualifier(), new byte[0], loadedVK.getTimestamp());
    }

    @Override
    public Value getTopValue() {
        return new Value(loadedVK.getValue());
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return null;
    }

}
