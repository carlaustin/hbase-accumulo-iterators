package org.apache.iterators;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.iterators.exampleiterators.ReplaceValueIterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;


public class AccumuloIteratorsObserver extends BaseRegionObserver {

    /**
     * Equivalent to a minor compaction in Accumulo, a flush writes memory based keys and values into a file
     * @param c
     * @param store
     * @param memstoreScanner
     * @param s
     * @return
     * @throws IOException
     */
    @Override
    public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {

        if (!"test".equals(store.getTableName())) {
            return null;
        }

        KeyValueScannerIteratorWrapper wrapper = new KeyValueScannerIteratorWrapper(Collections.singletonList(memstoreScanner), store);
        wrapper.init(null, null, null);
        ReplaceValueIterator replace = new ReplaceValueIterator();
        replace.init(wrapper, null, null);
        IteratorInternalScannerWrapper wrappedScanner = new IteratorInternalScannerWrapper(replace);
        return wrappedScanner;
    }

    /**
     * Equivalent to a major compaction in Accumulo, there are two types of this compaction in HBase minor and major.
     * Minor is actually like an accumulo major but doesn't drop deletes or expired cells, major is like a normal accumulo major.
     * @param c
     * @param store
     * @param scanners
     * @param scanType
     * @param earliestPutTs
     * @param s
     * @param request
     * @return
     * @throws IOException
     */
    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s, CompactionRequest request) throws IOException {

        if (!"test".equals(store.getTableName())) {
            return null;
        }

        KeyValueScannerIteratorWrapper wrapper = new KeyValueScannerIteratorWrapper(scanners, store);
        wrapper.init(null, null, null);
        ReplaceValueIterator replace = new ReplaceValueIterator();
        replace.init(wrapper, null, null);

        IteratorInternalScannerWrapper wrappedScanner = new IteratorInternalScannerWrapper(replace);
        return wrappedScanner;
    }

    /**
     * Equivalent to a read time scan in accumulo, placing an iterator in here will be equivalent to read time iterators.
     * @param c
     * @param store
     * @param scan
     * @param targetCols
     * @param s
     * @return
     * @throws IOException
     */
    @Override
    public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s) throws IOException {
        if (!"test".equals(store.getTableName())) {
            return null;
        }

        KeyValueScannerIteratorWrapperForStoreScanner wrapper = new KeyValueScannerIteratorWrapperForStoreScanner(new StoreScanner(store, store.getScanInfo(), scan, targetCols), store);
        wrapper.init(null, null, null);
        ReplaceValueIterator replace = new ReplaceValueIterator();
        replace.init(wrapper, null, null);
        //Could continue to add iterators to the chain as below
        //AnotherInterator another = new AnotherIterator();
        //another.init(replace, null, null);
        //...
        //AnotherInterator finalIteratorInChain = new AnotherIterator();
        //finalIteratorInChain.init(previousIterator, null, null);
        IteratorToKeyValueScanner wrappedScanner = new IteratorToKeyValueScanner(replace);
        //with a chain the call becomes
        //IteratorToKeyValueScanner wrappedScanner = new IteratorToKeyValueScanner(finalIteratorInChain);
        return wrappedScanner;
    }
}
