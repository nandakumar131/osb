package org.apache.ozone.snapshot;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

import java.io.IOException;

public interface OMDB {

  default OzoneConfiguration getOzoneConfiguration() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, "false");
    return conf;
  }

  Table<String, OmKeyInfo> getKeyTable() throws IOException;

  void close() throws Exception;
}
