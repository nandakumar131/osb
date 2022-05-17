package org.apache.ozone.rocksdb;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class OMSSTFileReader {

    final String dbLocation;
    final List<ColumnFamilyHandle> columnFamilyHandles;
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors;
    private List<LiveFileMetaData> fileMetaData;


    public OMSSTFileReader(String dbLocation) throws RocksDBException {
        this.columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(
                OmMetadataManagerImpl.KEY_TABLE.getBytes(
                        StandardCharsets.UTF_8)));
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(
                        "default".getBytes(
                                StandardCharsets.UTF_8)));
        this.dbLocation = dbLocation;
        this.columnFamilyHandles = new ArrayList<>();
        try (final DBOptions options = new DBOptions()) {
            try (final RocksDB rocksDB = RocksDB.openReadOnly(options, dbLocation,
                    columnFamilyDescriptors, columnFamilyHandles)) {
                this.fileMetaData = rocksDB.getLiveFilesMetaData();
            }
        }

    }

    public void printMetadata() {
        for (LiveFileMetaData data : fileMetaData) {
            System.out.println("File Name     : " + data.fileName());
            System.out.println("Table Name    : " + new String(data.columnFamilyName()));
            System.out.println("File size     : " + data.numEntries());
            System.out.println("Number of keys: " + data.numEntries());
            System.out.println("----------------------------");

        }
    }

    public void printKeyTable(int noOfRowsToPrint) throws RocksDBException, InvalidProtocolBufferException {
        int rowsPrinted = 0;
        for (LiveFileMetaData data : fileMetaData) {
            System.out.println("Printing from: " + data.fileName());
            System.out.println("*****************************");
            String file = data.path() + data.fileName();
            SstFileReader sstFileReader = new SstFileReader(new Options());
            sstFileReader.open(file);
            SstFileReaderIterator iterator = sstFileReader.newIterator(new ReadOptions());
            iterator.seekToFirst();
            while (iterator.isValid()) {
                System.out.println("key : " + new String(iterator.key()));
                System.out.println("Value: ");
                System.out.println(OzoneManagerProtocolProtos.KeyInfo.parseFrom(iterator.value()));
                iterator.next();
                rowsPrinted++;
                if (rowsPrinted >= noOfRowsToPrint) {
                    break;
                }
            }
            iterator.close();
            sstFileReader.close();
            if (rowsPrinted >= noOfRowsToPrint) {
                break;
            }
        }

    }
}
