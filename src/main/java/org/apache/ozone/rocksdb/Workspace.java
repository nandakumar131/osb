package org.apache.ozone.rocksdb;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class Workspace {
    public static void main(String[] args) throws Exception {

        TarArchiveInputStream tarInput = new TarArchiveInputStream(
                new GzipCompressorInputStream(
                        Workspace.class.getClassLoader()
                                .getResourceAsStream("title.tar.gz")));
        TarArchiveEntry currentEntry = tarInput.getNextTarEntry();
        BufferedReader br;
        while (currentEntry != null) {
            br = new BufferedReader(new InputStreamReader(tarInput));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            currentEntry = tarInput.getNextTarEntry();
        }

    }

}
