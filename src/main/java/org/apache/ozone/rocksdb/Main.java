package org.apache.ozone.rocksdb;

public class Main {

    public static void main(String[] args) throws Exception {
        //OMKeyTableWriter keyTableWriter = new OMKeyTableWriter("/Users/nvadivelu/om");
        //keyTableWriter.generate();

        OMSSTFileReader fileReader = new OMSSTFileReader("/Users/nvadivelu/om/om.db");
        fileReader.printMetadata();
        fileReader.printKeyTable(10);
    }
}
