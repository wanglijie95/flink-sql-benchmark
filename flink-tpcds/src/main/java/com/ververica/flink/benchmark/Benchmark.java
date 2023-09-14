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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.ververica.flink.benchmark.QueryUtil.getQueries;

public class Benchmark {

	// common conf
	public static final Option LOCATION = new Option("l", "location", true,
			"sql query path.");

    public static final Option QUERIES = new Option("q", "queries", true,
			"sql query names. If the value is 'all', all queries will be executed.");

    public static final Option ITERATIONS = new Option("i", "iterations", true,
			"The number of iterations that will be run per case, default is 1.");

    public static final Option PARALLELISM = new Option("p", "parallelism", true,
			"The parallelism, default is 800.");

    public static final Option MODE =
			new Option("m", "mode", true, "mode: 'execute' or 'explain' or 'print-job-graph'");

	// hive conf
	public static final Option HIVE_CONF = new Option("c", "hive_conf", true,
			"conf of hive.");
	public static final Option DATABASE = new Option("d", "database", true,
			"database of hive.");

	// paimon conf
	public static final Option PAIMON_WAREHOUSE =
			new Option("pw", "paimon_warehouse", true, "warehouse of paimon.");
	public static final Option PAIMON_DATABASE =
			new Option("pd", "paimon_database", true, "database of paimon.");
	public static final Option PAIMON_SCAN_PARALLELISM =
			new Option("pcp", "paimon_scan_parallelism", true, "scan parallelism of paimon.");
	public static final Option PAIMON_SINK_PARALLELISM =
			new Option("pkp", "paimon_sink_parallelism", true, "scan parallelism of paimon.");


	private Benchmark() {}

	public static void runQueries(TableEnvironment tEnv, CommandLine line, String externalDatabase) throws ParseException {
        String mode = line.getOptionValue(MODE.getOpt(), "execute");
        LinkedHashMap<String, String> queries =
                getQueries(
                        line.getOptionValue(LOCATION.getOpt()),
                        line.getOptionValue(QUERIES.getOpt()));

		if (externalDatabase != null) {
			tEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + externalDatabase);
		}
		System.out.println("After create database " + externalDatabase);

		if ("explain".equals(mode)) {
			explain(tEnv, queries);
		} else {
			run(tEnv, queries, Integer.parseInt(line.getOptionValue(ITERATIONS.getOpt(), "1")), mode.equals("print-job-graph"), externalDatabase, line.getOptionValue(PAIMON_SINK_PARALLELISM.getOpt()));
		}
	}

	private static void run(TableEnvironment tEnv, LinkedHashMap<String, String> queries, int iterations, boolean printJobGraph, String externalDatabase, String sinkParallelism) {
		List<Tuple2<String, Long>> bestArray = new ArrayList<>();
		queries.forEach((name, sql) -> {
			tEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, name);
			System.out.println("Start run query: " + name);
			Runner runner = new Runner(name, sql, iterations, tEnv, printJobGraph, externalDatabase, sinkParallelism);
			runner.run(bestArray);
		});
		printSummary(bestArray);
	}

    private static void explain(TableEnvironment tEnv, LinkedHashMap<String, String> queries) {
        List<Tuple2<String, Long>> bestArray = new ArrayList<>();
        queries.forEach(
                (name, sql) -> {
                    System.out.println("Start explain query: " + name);
					String plan = tEnv.explainSql(sql, ExplainDetail.ESTIMATED_COST);
					if (plan.contains("RuntimeFilter")) {
						System.out.println("Query " + name + " contains RuntimeFilter: Y");
					} else {
						System.out.println("Query " + name + " contains RuntimeFilter: N");
					}
                    System.out.println(plan);
                });
        printSummary(bestArray);
    }

	private static void printSummary(List<Tuple2<String, Long>> bestArray) {
		if (bestArray.isEmpty()) {
			return;
		}
		System.err.println("--------------- tpcds Results ---------------");
		int itemMaxLength = 20;
		System.err.println();
		long total = 0L;
		double product = 1d;
		printLine('-', "+", itemMaxLength, "", "");
		printLine(' ', "|", itemMaxLength, " " + "tpcds sql", " Time(ms)");
		printLine('-', "+", itemMaxLength, "", "");

		for (Tuple2<String, Long> tuple2 : bestArray) {
			printLine(' ', "|", itemMaxLength, tuple2.f0, String.valueOf(tuple2.f1));
			total += tuple2.f1;
			product = product * tuple2.f1 / 1000d;
		}

		printLine(' ', "|", itemMaxLength, "Total", String.valueOf(total));
		printLine(' ', "|", itemMaxLength, "Average", String.valueOf(total / bestArray.size()));
		printLine(' ', "|", itemMaxLength, "GeoMean", String.valueOf((java.lang.Math.pow(product, 1d / bestArray.size()) * 1000)));
		printLine('-', "+", itemMaxLength, "", "");

		System.err.println();
	}

	static void printLine(
			char charToFill,
			String separator,
			int itemMaxLength,
			String... items) {
		StringBuilder builder = new StringBuilder();
		for (String item : items) {
			builder.append(separator);
			builder.append(item);
			int left = itemMaxLength - item.length() - separator.length();
			for (int i = 0; i < left; i++) {
				builder.append(charToFill);
			}
		}
		builder.append(separator);
		System.err.println(builder.toString());
	}

	public static Options getOptions() {
		Options options = new Options();
		options.addOption(LOCATION);
		options.addOption(QUERIES);
		options.addOption(ITERATIONS);
		options.addOption(PARALLELISM);
        options.addOption(MODE);

		options.addOption(HIVE_CONF);
		options.addOption(DATABASE);

		options.addOption(PAIMON_WAREHOUSE);
		options.addOption(PAIMON_DATABASE);
		options.addOption(PAIMON_SCAN_PARALLELISM);
		options.addOption(PAIMON_SINK_PARALLELISM);
		return options;
	}
}
