package org.apache.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IteratorInternalScannerWrapper implements InternalScanner {

    private final SortedKeyValueIterator<Key, Value> wrapped;

    public IteratorInternalScannerWrapper(SortedKeyValueIterator<Key, Value> wrapped) {
        this.wrapped = wrapped;
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
    public boolean next(List<KeyValue> results, int limit) throws IOException {
        return next(results, limit, null);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
        if (!this.wrapped.hasTop() || this.wrapped.getTopKey() == null) {
            return false;
        }
        Text currentRow = this.wrapped.getTopKey().getRow();
        while (this.wrapped.hasTop() && this.wrapped.getTopKey().getRow().equals(currentRow)) {
            result.add(new KeyValue(this.wrapped.getTopKey().getRow().getBytes(),
                    this.wrapped.getTopKey().getColumnFamily().getBytes(),
                    this.wrapped.getTopKey().getColumnQualifier().getBytes(),
                    this.wrapped.getTopKey().getTimestamp(),
                    this.wrapped.getTopValue().get()));
            if (limit > 0 && result.size() >= limit) {
                break;
            }
            this.wrapped.next();
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
