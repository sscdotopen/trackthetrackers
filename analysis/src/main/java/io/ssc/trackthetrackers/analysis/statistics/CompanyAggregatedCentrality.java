/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Hung Chang
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.analysis.statistics;

import io.ssc.trackthetrackers.Config;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Aggregate Centrality to company level
public class CompanyAggregatedCentrality {
	private static String argPathTrackerCentrality = "/home/sendoh/datasets/ThirdPartyAggregatedCentrality";
	private static String argPathToDomainCompany = "/home/sendoh/datasets/DomainsAndCompany.csv";

	private static String argPathOut = Config.get("analysis.results.path")
			+ "companyAggregatedCentrality";

	private static int topK = 35;

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputTrackerCentrality = env
				.readTextFile(argPathTrackerCentrality);

		DataSource<String> inputDomainCompany = env
				.readTextFile(argPathToDomainCompany);

		DataSet<Tuple2<String, Double>> trackerCentrality = inputTrackerCentrality
				.flatMap(new ValueReader());

		DataSet<Tuple2<String, String>> domainCompany = inputDomainCompany
				.flatMap(new DomainReader());

		// Join company and domain
		DataSet<Tuple3<String, String, Double>> companyTrackerCentrality = trackerCentrality
				.join(domainCompany).where(0).equalTo(0)
				.flatMap(new ProjectCompanyTrackerValue());

		DataSet<Tuple2<String, Double>> companyCentrality = companyTrackerCentrality
				.groupBy(0).aggregate(Aggregations.SUM, 2)
				.<Tuple2<String, Double>> project(0, 2);

		// Output 1, ID, degree for group by 0
		DataSet<Tuple3<Long, String, Double>> topKMapper = companyCentrality
				.flatMap(new TopKMapper());

		// Get topK
		DataSet<Tuple3<Long, String, Double>> topKReducer = topKMapper
				.groupBy(0).sortGroup(2, Order.DESCENDING).first(topK);

		// Node ID joins with node's name
		DataSet<Tuple2<String, Double>> topKProject = topKReducer
				.<Tuple2<String, Double>> project(1, 2);

		topKProject.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class ValueReader implements
			FlatMapFunction<String, Tuple2<String, Double>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s,
				Collector<Tuple2<String, Double>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String node = tokens[0];
				Double value = Double.parseDouble(tokens[1]);
				collector.collect(new Tuple2<String, Double>(node, value));
			}
		}
	}

	// Keep domains' full name
	public static class DomainFullReader implements
			FlatMapFunction<String, Tuple2<String, String>> {

		@Override
		public void flatMap(String input,
				Collector<Tuple2<String, String>> collector) throws Exception {
			if (!input.startsWith("%")) {
				String domain = input.substring(0, input.indexOf(","));
				String company = input.substring(input.indexOf(",") + 1).trim();
				collector.collect(new Tuple2<String, String>(domain, company));
			}
		}
	}

	// Symbol only
	public static class DomainReader implements
			FlatMapFunction<String, Tuple2<String, String>> {

		private static final Pattern SEPARATOR = Pattern.compile("[\t,]");

		@Override
		public void flatMap(String s,
				Collector<Tuple2<String, String>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String domain = tokens[0];
				String company = tokens[1];
				collector.collect(new Tuple2<String, String>(domain, company));
			}
		}
	}

	public static class ProjectCompanyTrackerValue
			implements
			FlatMapFunction<Tuple2<Tuple2<String, Double>, Tuple2<String, String>>, Tuple3<String, String, Double>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<String, Double>, Tuple2<String, String>> value,
				Collector<Tuple3<String, String, Double>> collector)
				throws Exception {
			String company = value.f1.f1;
			String tracker = value.f0.f0;
			double centrality = value.f0.f1;
			collector.collect(new Tuple3<String, String, Double>(company,
					tracker, centrality));
		}
	}

	public static class ProjectArcValues
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple3<String, Long, Double>>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple3<String, Long, Double>> input,
				Collector<Tuple2<Long, Double>> collector) throws Exception {
			Long id = input.f0.f0;
			Double value = input.f1.f2;
			collector.collect(new Tuple2<Long, Double>(id, value));
		}
	}

	public static class TopKMapper
			implements
			FlatMapFunction<Tuple2<String, Double>, Tuple3<Long, String, Double>> {

		@Override
		public void flatMap(Tuple2<String, Double> tuple,
				Collector<Tuple3<Long, String, Double>> collector)
				throws Exception {
			collector.collect(new Tuple3<Long, String, Double>((long) 1,
					tuple.f0, tuple.f1));
		}
	}

	public static class ProjectIDWithName
			implements
			FlatMapFunction<Tuple2<Tuple3<Long, Long, Double>, Tuple2<String, Long>>, Tuple2<String, Double>> {

		@Override
		public void flatMap(
				Tuple2<Tuple3<Long, Long, Double>, Tuple2<String, Long>> value,
				Collector<Tuple2<String, Double>> collector) throws Exception {
			collector.collect(new Tuple2<String, Double>(value.f1.f0,
					value.f0.f2));
		}
	}
}
