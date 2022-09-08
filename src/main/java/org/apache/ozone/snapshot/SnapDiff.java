/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.ozone.snapshot;

import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.bucket.BucketUri;
import picocli.CommandLine;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@CommandLine.Command(name="snap-diff",
    description = "Command to generate ozone snapshot diffs.",
    versionProvider = SnapshotVersionProvider.class,
    mixinStandardHelpOptions = true)
public class SnapDiff implements Runnable {

  private final CommandLine commandLine;

  @CommandLine.Option(names = {"-p", "--dbPath"},
      description = "Path to store db.")
  private String path = "/tmp";

  @CommandLine.Option(names = {"-fs", "--fromSnapshot"},
      description = "Old snapshot from which snap-diff should be calculated.")
  private String from;

  @CommandLine.Option(names = {"-ts", "--toSnapshot"},
      description = "New snapshot against which snap-diff should be calculated.")
  private String to;

  @CommandLine.Option(names = {"-b", "--bucket"},
      description = "Bucket to which both the from & to snapshot"
          + " corresponds. Format: 'volume/bucket' ",
      converter = BucketUri.class,
      required = true)
  private OzoneAddress bucket;

  private SnapDiff() {
    this.commandLine = new CommandLine(this);
  }

  @Override
  public void run() {
    try {
      final OzoneManager ozoneManager = new OzoneManager(path);
      String volumeName = bucket.getVolumeName();
      String bucketName = bucket.getBucketName();
      writeSnapDiffToFile(
          ozoneManager.getSnapshotManager().getSnapshotDiffManager()
              .getSnapshotDiff(volumeName, bucketName, from, to));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }  }

  private void writeSnapDiffToFile(List<String> snapshotDiff)
      throws IOException {
    String fileName = path + "/" + from + "--" + to + ".diff";
    FileWriter writer = new FileWriter(fileName);
    BufferedWriter buffer = new BufferedWriter(writer);
    for (String line : snapshotDiff) {
      buffer.write(line);
      buffer.newLine();
    }
    buffer.close();
    System.out.println("Snap diff File : " + fileName);
  }

  private int execute(final String[] args) {
    return commandLine.execute(args);
  }


}
