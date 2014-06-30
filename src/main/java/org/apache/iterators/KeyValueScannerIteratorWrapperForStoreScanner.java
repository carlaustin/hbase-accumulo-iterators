package org.apache.iterators;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class KeyValueScannerIteratorWrapperForStoreScanner implements SortedKeyValueIterator<Key, Value> {

    private final StoreScanner scanner;
    private final Store store;
    private boolean anyMore = true;
    private List<KeyValue> loadedKV = new ArrayList<KeyValue>();
    private KeyValue thisKV;

    public KeyValueScannerIteratorWrapperForStoreScanner(StoreScanner scanner, Store store) {
        this.scanner = scanner;
        this.store = store;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        next();
    }

    @Override
    public boolean hasTop() {
        return anyMore;
    }

    @Override
    public void next() throws IOException {
        //does the list have any more entries?
        if (loadedKV.size() > 0) {
            thisKV = loadedKV.remove(0);
        } else {
            loadedKV.clear();
            anyMore = this.scanner.next(loadedKV);
            if (anyMore) {
                if (loadedKV.size() == 0) {
                    next();
                } else {
                    thisKV = loadedKV.remove(0);
                }
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.scanner.seek(new KeyValue(range.getStartKey().getRow().getBytes(), range.getStartKey().getColumnFamily().getBytes(), range.getStartKey().getColumnQualifier().getBytes()));
        next();
    }

    @Override
    public Key getTopKey() {
        return new Key(thisKV.getRow(), thisKV.getFamily(), thisKV.getQualifier(), new byte[0], thisKV.getTimestamp());
    }

    @Override
    public Value getTopValue() {
        return new Value(thisKV.getValue());
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return null;
    }

}
