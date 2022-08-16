package org.apache.ozone.snapshot;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.HDDS_DEFAULT_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

public class OMDBWithCompactionListener implements OMDB {

  private OzoneConfiguration configuration;
  private DBStore dbStore;

  public OMDBWithCompactionListener(String dbLocation) throws IOException {
    this.configuration = getOzoneConfiguration();
    this.dbStore = getDBStore(new File(dbLocation), OM_DB_NAME);

  }

  private DBStore getDBStore(File metaDir, String dbName) throws IOException {
    RocksDBConfiguration rocksDBConfiguration =
        configuration.getObject(RocksDBConfiguration.class);

    DBProfile dbProfile = configuration.getEnum(HDDS_DB_PROFILE,
        HDDS_DEFAULT_DB_PROFILE);

    DBOptions dBOptions = dbProfile.getDBOptions();
    dBOptions.setListeners(Collections.singletonList(getCompactionEventListener()));

    DBStoreBuilder dbStoreBuilder = DBStoreBuilder.newBuilder(configuration,
            rocksDBConfiguration).setName(dbName)
        .setPath(Paths.get(metaDir.getPath()));

    dbStoreBuilder.setDBOptions(dBOptions);
    return OmMetadataManagerImpl.addOMTablesAndCodecs(dbStoreBuilder).build();
  }

  private AbstractEventListener getCompactionEventListener() {
    return new AbstractEventListener() {
      @Override
      public void onCompactionCompleted(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
        System.out.println(compactionJobInfo.compactionReason());
        System.out.println("List of input files:");
        for (String file : compactionJobInfo.inputFiles()) {
          System.out.println(file);
        }
        System.out.println("List of output files:");
        for (String file : compactionJobInfo.outputFiles()) {
          System.out.println(file);
        }
      }
    };
  }

  @Override
  public Table<String, OmKeyInfo> getKeyTable() throws IOException {
    return dbStore.getTable(KEY_TABLE, String.class, OmKeyInfo.class);
  }

  @Override
  public void close() throws Exception {
    dbStore.close();
  }
}
