package org.apache.ozone.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import org.rocksdb.RocksDBException;

public class Main {

    private static final String DB_LOCATION = "/Users/nvadivelu/Workspace/data/om/current";
    private static final String CHECKPOINT_DB_LOCATION = "/Users/nvadivelu/Workspace/data/om/checkpoints";

    public static void main(String[] args) throws Exception {

    }

    private static void populateKeyTable() throws Exception {
        final OMKeyTableWriter keyTableWriter = new OMKeyTableWriter(OMDBFactory.createOMDBWithCompactionListener(DB_LOCATION));
        keyTableWriter.generateRandom(100_000_000);
        keyTableWriter.close();
    }

    private static void printNumberOfKeys(String dbLocation) throws Exception {
        final RDBHelper rdbHelper = new RDBHelper(dbLocation);
        System.out.println("Number of keys: " + rdbHelper.getKeyCount());
        rdbHelper.close();
    }
}
