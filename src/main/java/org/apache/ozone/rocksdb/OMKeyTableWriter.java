package org.apache.ozone.rocksdb;

import com.sun.tools.javac.util.List;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class OMKeyTableWriter {

    private OMMetadataManager omMetadataManager;
    private OzoneConfiguration configuration;
    private Table<String, OmKeyInfo> keyTable;

    private String volume;
    private String bucket;

    public OMKeyTableWriter(String dbLocation) throws IOException {
        this.configuration = getOzoneConfiguration();
        this.configuration.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbLocation);
        this.omMetadataManager = new OmMetadataManagerImpl(configuration);
        this.keyTable = omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE);

        this.volume = "movies";
        this.bucket = "hollywood";
    }

    private OzoneConfiguration getOzoneConfiguration() {
        final OzoneConfiguration conf = new OzoneConfiguration();
        conf.set(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, "false");
        return conf;
    }

    public void generate() throws Exception {
        TarArchiveInputStream tarInput = new TarArchiveInputStream(
                new GzipCompressorInputStream(
                        getClass().getClassLoader()
                                .getResourceAsStream("title.tar.gz")));

        TarArchiveEntry currentEntry = tarInput.getNextTarEntry();
        BufferedReader br;
        while (currentEntry != null) {
            br = new BufferedReader(new InputStreamReader(tarInput));
            final String keyLocation = "/" + volume + "/" + bucket + "/";
            String title;
            while ((title = br.readLine()) != null) {
                keyTable.put(keyLocation + title, getKeyInfo(title));
            }
            currentEntry = tarInput.getNextTarEntry();
        }
        keyTable.close();
        omMetadataManager.stop();
    }

    private OmKeyInfo getKeyInfo(final String key) {
        final OmKeyInfo.Builder builder = new OmKeyInfo.Builder();
        builder.setVolumeName(volume)
                .setBucketName(bucket)
                .setKeyName(key)
                .setDataSize(10000000)
                .setCreationTime(System.currentTimeMillis())
                .setModificationTime(System.currentTimeMillis())
                .setReplicationConfig(ReplicationConfig.parse(
                        ReplicationType.RATIS,"THREE", configuration))
                .setAcls(List.of(OzoneAcl.parseAcl("user:imdb:rw")));
        return builder.build();
    }
}
