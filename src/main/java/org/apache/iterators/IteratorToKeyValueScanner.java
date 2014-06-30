package org.apache.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;

public class IteratorToKeyValueScanner implements KeyValueScanner, InternalScanner {

    private final SortedKeyValueIterator<Key, Value> iterator;
    private final WholeRowIterator wholeRowIterator = new WholeRowIterator();

    public IteratorToKeyValueScanner(SortedKeyValueIterator<Key, Value> iterator) {
        this.iterator = iterator;
        try {
            this.wholeRowIterator.init(iterator, null, null);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public KeyValue peek() {
        if (!iterator.hasTop()) {
            return null;
        }
        return new KeyValue(iterator.getTopKey().getRow().getBytes(), iterator.getTopKey().getColumnFamily().getBytes(), iterator.getTopKey().getColumnQualifier().getBytes(), iterator.getTopKey().getTimestamp(), iterator.getTopValue().get());
    }

    @Override
    public KeyValue next() throws IOException {
        KeyValue kv = new KeyValue(iterator.getTopKey().getRow().getBytes(), iterator.getTopKey().getColumnFamily().getBytes(), iterator.getTopKey().getColumnQualifier().getBytes(), iterator.getTopKey().getTimestamp(), iterator.getTopValue().get());
        iterator.next();
        return kv;
    }

    @Override
    public boolean seek(KeyValue keyValue) throws IOException {
        wholeRowIterator.seek(new Range(new Key(keyValue.getRow(), keyValue.getFamily(), keyValue.getQualifier(), new byte[0], keyValue.getTimestamp()), null), null, false);
        return iterator.hasTop();
    }

    @Override
    public boolean reseek(KeyValue keyValue) throws IOException {
        return seek(keyValue);
    }

    @Override
    public long getSequenceID() {
        return 0;
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
        return next(results, -1, null);
    }

    @Override
    public boolean next(List<KeyValue> results, String metric) throws IOException {
        return next(results, -1, metric);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit) throws IOException {
        return next(result, limit, null);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
        wholeRowIterator.next();
        SortedMap<Key, Value> row = WholeRowIterator.decodeRow(wholeRowIterator.getTopKey(), wholeRowIterator.getTopValue());
        int count = 0;
        for (Key key : row.keySet()) {
            if (limit > 0 && count > limit) {
                break;
            }
            result.add(new KeyValue(key.getRow().getBytes(), key.getColumnFamily().getBytes(), key.getColumnQualifier().getBytes(), key.getTimestamp(), row.get(key).get()));
        }
        return wholeRowIterator.hasTop();
    }

    @Override
    public void close() {
    }

    @Override
    public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> bytes, long l) {
        return true;
    }

    @Override
    public boolean requestSeek(KeyValue keyValue, boolean b, boolean b2) throws IOException {
        return seek(keyValue);
    }

    @Override
    public boolean realSeekDone() {
        return true;
    }

    @Override
    public void enforceSeek() throws IOException {
        throw new NotImplementedException("enforceSeek must not be called on a " +
                "non-lazy scanner");
    }

    @Override
    public boolean isFileScanner() {
        return false;
    }
}
