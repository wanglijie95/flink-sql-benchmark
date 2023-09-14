package com.ververica.flink.benchmark;

import org.apache.commons.cli.*;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hive.common.util.HiveVersionInfo;

import static com.ververica.flink.benchmark.Benchmark.*;
import static java.util.Objects.requireNonNull;

public class HiveBenchmark {

    public static void main(String[] args) throws ParseException {
        System.out.println("Running HiveBenchmark, args: " + String.join(" ", args));

        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);

        TableEnvironment tEnv =
                setUpEnv(
                        requireNonNull(line.getOptionValue(HIVE_CONF.getOpt())),
                        requireNonNull(line.getOptionValue(DATABASE.getOpt())));
        Benchmark.runQueries(tEnv, line);
    }

    private static TableEnvironment setUpEnv(String hiveConf, String database) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.getConfig().addConfiguration(GlobalConfiguration.loadConfiguration());

        HiveCatalog catalog =
                new HiveCatalog("hive", database, hiveConf, HiveVersionInfo.getVersion());
        tEnv.registerCatalog("hive", catalog);
        tEnv.useCatalog("hive");
        return tEnv;
    }
}
