package org.apache.ozone.snapshot;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

import java.io.IOException;

public class OMDBImpl implements OMDB {

  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration configuration;


  public OMDBImpl(String dbLocation) throws IOException {
    this.configuration = getOzoneConfiguration();
    this.configuration.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbLocation);
    this.omMetadataManager = new OmMetadataManagerImpl(configuration);
  }

  @Override
  public Table<String, OmKeyInfo> getKeyTable() {
    return omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE);
  }

  @Override
  public void close() throws Exception {
    omMetadataManager.stop();
  }
}
