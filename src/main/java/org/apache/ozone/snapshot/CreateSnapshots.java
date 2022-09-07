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

import com.google.common.base.Preconditions;
import picocli.CommandLine;

import java.util.UUID;

@CommandLine.Command(name="create-snapshots",
    description = "Command to create ozone snapshots.",
    versionProvider = SnapshotVersionProvider.class,
    mixinStandardHelpOptions = true)
public class CreateSnapshots implements Runnable {

  public static final double DEFAULT_DELETE_OPS_PERCENT = 0.3;
  public static final double DEFAULT_RENAME_OPS_PERCENT = 0.3;
  private final CommandLine commandLine;

  @CommandLine.Option(names = {"-p", "--dbPath"},
      description = "Path to store db.")
  private String path = "/tmp";

  @CommandLine.Option(names = {"-n", "--noOfSnapshots"},
      description = "Number of Snapshots to create.")
  private int nos = 5;

  @CommandLine.Option(names = {"-k", "--keysPerSnapshot"},
      description = "Number of keys per Snapshot.")
  private int keyCount = 100000;

  @CommandLine.Option(names = {"-nv", "--volumes"},
      description = "Number of Volumes")
  private int numVolumes = 3;

  @CommandLine.Option(names = {"-nb", "--buckets"},
      description = "Number of Volumes")
  private int numBuckets = 5;

  @CommandLine.Option(names = {"-do", "--deletes"},
      description = "Percentage of Delete operations in consecutive snapshots")
  private double deleteOps= DEFAULT_DELETE_OPS_PERCENT;

  @CommandLine.Option(names = {"-ro", "--renames"},
      description = "Percentage of Rename operations in consecutive snapshots")
  private double renameOps= DEFAULT_RENAME_OPS_PERCENT;


  private static CreateSnapshots getInstance() {
    return new CreateSnapshots();
  }

  private CreateSnapshots() {
    this.commandLine = new CommandLine(this);
  }

  @Override
  public void run() {
    if (!validateOpsPercentage(deleteOps,renameOps)){
      // fallback to defaults
      fallbackToDefaultPercentages();
    }
    try {
      final OzoneManager ozoneManager = new OzoneManager(path);
      OMKeyTableWriter keyTableWriter = ozoneManager.getKeyTableWriter();
      createKeysAndTakeSnapshot(ozoneManager, keyTableWriter);
      for (int i = 0; i < nos-1 ; i++) {
        performOpsAndTakeSnapshot(ozoneManager, keyTableWriter);
      }
      ozoneManager.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void fallbackToDefaultPercentages() {
    deleteOps = DEFAULT_DELETE_OPS_PERCENT;
    renameOps = DEFAULT_RENAME_OPS_PERCENT;
  }

  private void performOpsAndTakeSnapshot(OzoneManager ozoneManager,
      OMKeyTableWriter keyTableWriter) throws Exception {
    double createOps = 1-(deleteOps+renameOps);
    // First perform deletes
    keyTableWriter.deleteKeys(deleteOps,keyCount);

    //Now renames
    keyTableWriter.renameKeys(renameOps,keyCount);

    //Creates
    int numKeyCreates = (int)Math.ceil(createOps*keyCount);
    keyTableWriter.generateRandom(numVolumes,numBuckets,numKeyCreates);
    // take snapshot
    ozoneManager.getOMDB().createSnapshot(UUID.randomUUID().toString());
  }

  private void createKeysAndTakeSnapshot(OzoneManager ozoneManager,
      OMKeyTableWriter keyTableWriter) throws Exception {
    keyTableWriter.generateRandom(numVolumes,numBuckets,keyCount);
    ozoneManager.getOMDB().createSnapshot(UUID.randomUUID().toString());
  }

  private boolean validateOpsPercentage(double deleteOps, double renameOps) {
    if (!(deleteOps + renameOps <= 1)){
      return false;
    }
    double createOps = 1 -(deleteOps+renameOps);
    Preconditions.checkArgument(deleteOps<renameOps+createOps
        ,"Deletes must be less than the sum of renames & creates");
    return true;
  }

  private int execute(final String[] args) {
    return commandLine.execute(args);
  }

}