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

import picocli.CommandLine;

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

  private static SnapDiff getInstance() {
    return new SnapDiff();
  }

  private SnapDiff() {
    this.commandLine = new CommandLine(this);
  }

  @Override
  public void run() {
    try {
      final OzoneManager ozoneManager = new OzoneManager(path);
      ozoneManager.getSnapshotManager().getSnapshotDiffManager()
          .getSnapshotDiff(null, null, from, to).forEach(System.out::println);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }  }

  private int execute(final String[] args) {
    return commandLine.execute(args);
  }

  public static void main(final String[] args) {
    SnapDiff snapDiff = getInstance();
    snapDiff.path = "/Users/nvadivelu/Workspace/data/";
    snapDiff.from = "om.dbf9e9e4a6-02f3-4535-924e-adf6f26e3275";
    snapDiff.to = "om.db971efa7d-2892-4813-8a61-5f1527308b6d";
    snapDiff.run();
  }

}
