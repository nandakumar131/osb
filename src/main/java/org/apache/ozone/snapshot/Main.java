/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.ozone.snapshot;

import picocli.CommandLine;

@CommandLine.Command(name="oz",
    description = "Ozone Snapshot Tools.",
    versionProvider = SnapshotVersionProvider.class,
    subcommands = {
        CreateSnapshots.class,
        ListSnapshots.class,
        SnapDiff.class
    },
    mixinStandardHelpOptions = true,
    hidden = true)
public class Main implements Runnable {

    private final CommandLine commandLine;

    private static Main getInstance() {
        return new Main();
    }
    private Main() {
        this.commandLine = new CommandLine(this);
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(commandLine, "Missing Command!");
    }

    private int execute(final String[] args) {
        return commandLine.execute(args);
    }

    public static void main(final String[] args) {
        System.exit(Main.getInstance().execute(args));
    }

    private static void printNumberOfKeys(String dbLocation) throws Exception {
        final RDBHelper rdbHelper = new RDBHelper(dbLocation);
        System.out.println("Number of keys: " + rdbHelper.getKeyCount());
        rdbHelper.close();
    }
}
