/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.benchmark;

import org.apache.commons.cli.*;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import static com.ververica.flink.benchmark.Benchmark.*;
import static java.util.Objects.requireNonNull;

public class PaimonBenchmark {

    public static void main(String[] args) throws ParseException {
        System.out.println("Running PaimonBenchmark, args: " + String.join(" ", args));

        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);

        TableEnvironment tEnv =
                setUpEnv(
                        requireNonNull(line.getOptionValue(PAIMON_WAREHOUSE.getOpt())),
                        requireNonNull(line.getOptionValue(PAIMON_DATABASE.getOpt())));
        Benchmark.runQueries(tEnv, line);
    }

    private static TableEnvironment setUpEnv(String paimonWarehouse, String paimonDatabase) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.getConfig().addConfiguration(GlobalConfiguration.loadConfiguration());

        // create paimon catalog
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon WITH (\n"
                                + "  'type' = 'paimon', \n"
                                + "  'warehouse' = '%s', \n"
                                + "  'default-database' = '%s')",
                        paimonWarehouse, paimonDatabase));
        tEnv.executeSql("USE CATALOG paimon");
        return tEnv;
    }
}
