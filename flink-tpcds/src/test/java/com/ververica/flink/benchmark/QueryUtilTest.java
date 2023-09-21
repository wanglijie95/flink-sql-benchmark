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
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.ververica.flink.benchmark.Benchmark.*;
import static com.ververica.flink.benchmark.QueryUtil.getQueries;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link QueryUtil}.
 */
public class QueryUtilTest {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Test
	public void testInternalAll() {
		LinkedHashMap<String, String> queries = getQueries(null, null);
		System.out.println(StringUtils.join(",", queries.keySet()));
		assertQueries(queries, 102);
	}

	@Test
	public void testInternalSelect() {
		LinkedHashMap<String, String> queries = getQueries(null, "q1.sql,q14b.sql,q95.sql");
		assertQueries(queries, 3);
	}

	@Test
	public void testAll() throws IOException {
		String dir = prepareOutFile();
		LinkedHashMap<String, String> queries = getQueries(dir, null);
		assertQueries(queries, 102);
	}

	@Test
	public void testSelect() throws IOException {
		String dir = prepareOutFile();
		LinkedHashMap<String, String> queries = getQueries(dir, "q1.sql,q14b.sql,q95.sql");
		assertQueries(queries, 3);
	}

	@Test
	public void testParseOptions() throws ParseException {
		Options options = getOptions();
		DefaultParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, new String[] {"--paimon_warehouse", "hdfs://master-1-1.c-f260d4b8d218a0fa.cn-beijing.emr.aliyuncs.com:9000/paimon-tpcds-partitioned",  "--paimon_database", "default",  "--queries", "q4.sql"}, false);
		String mode = line.getOptionValue(MODE.getOpt(), "execute");
		String location = line.getOptionValue(LOCATION.getOpt());
		String queries = line.getOptionValue(QUERIES.getOpt());
		System.out.println(mode);
		System.out.println(location);
		System.out.println(queries);

		System.out.println("q100.sql".split("\\.")[0]);
	}

	@Test
	public void test666() {
		LoggerConfig loggerConfig = ((LoggerContext) LogManager.getContext()).getConfiguration().getRootLogger();
		Map<String, Appender> appenders = loggerConfig.getAppenders();
		for (Appender appender : appenders.values()) {
			System.out.println(appender.getClass());
		}
	}

	private String prepareOutFile() throws IOException {
		LinkedHashMap<String, String> queries = getQueries(null, null);
		File dir = TEMPORARY_FOLDER.newFolder();
		for (Map.Entry<String, String> e : queries.entrySet()) {
			Files.write(
					new File(dir, e.getKey()).toPath(),
					e.getValue().getBytes(StandardCharsets.UTF_8));
		}
		return dir.getAbsolutePath();
	}

	private void assertQueries(LinkedHashMap<String, String> queries, int size) {
		assertEquals(size, queries.size());
		queries.forEach((name, sql) -> {
			assertNotNull(name);
			assertTrue(name.length() > 0);

			assertNotNull(sql);
			assertTrue(sql.length() > 0);
		});
	}
}
