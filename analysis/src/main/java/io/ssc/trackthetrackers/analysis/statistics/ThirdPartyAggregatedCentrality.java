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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Get the aggregated Centrality for each third party, and get the top K third party.
public class ThirdPartyAggregatedCentrality {

	private static String argPathToNodesAndValues = Config
			.get("webdatacommons.hostgraph-pr.unzipped");
	private static String argPathToPLD = Config
			.get("webdatacommons.pldfile.unzipped");
	private static String argPathToTrackingArcs = Config
			.get("analysis.trackingraphsample.path");
	private static String argPathOut = Config.get("analysis.results.path")
			+ "ThirdPartyAggregatedCentrality";

	private static int topK = 10000;

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputNodesAndValue = env
				.readTextFile(argPathToNodesAndValues);

		DataSource<String> inputTrackingArcs = env
				.readTextFile(argPathToTrackingArcs);

		DataSource<String> inputNodePLD = env.readTextFile(argPathToPLD);

		DataSet<Tuple2<String, Long>> pldNodes = inputNodePLD
				.flatMap(new NodeReader());

		DataSet<Tuple2<Long, Long>> trackingArcs = inputTrackingArcs
				.flatMap(new ArcReader());

		// Convert the input as (nodeName, value)
		DataSet<Tuple2<String, Double>> nodeAndValue = inputNodesAndValue
				.flatMap(new ValueReader());

		DataSet<Tuple2<String, Long>> sourceNode = trackingArcs
				.<Tuple1<Long>> project(0).join(pldNodes).where(0).equalTo(1)
				.flatMap(new ProjectSourceNodeAndID());

		// Get node ID (domain, ID, value)
		DataSet<Tuple3<String, Long, Double>> NodeIdAndValue = nodeAndValue
				.join(pldNodes).where(0).equalTo(0)
				.flatMap(new ProjectNameIDAndValues());

		// Get (src, des, value)
		DataSet<Tuple2<Long, Double>> arcsValues = trackingArcs
				.join(NodeIdAndValue).where(1).equalTo(1)
				.flatMap(new ProjectArcValues());

		DataSet<Tuple2<Long, Double>> trackerIDAndValue = arcsValues.groupBy(0)
				.aggregate(Aggregations.SUM, 1);

		// Output 1, ID, value
		DataSet<Tuple3<Long, Long, Double>> topKMapper = trackerIDAndValue
				.flatMap(new TopKMapper());

		// Get topK
		DataSet<Tuple3<Long, Long, Double>> topKReducer = topKMapper.groupBy(0)
				.sortGroup(2, Order.DESCENDING).first(topK);

		// Node ID joins with node's name
		DataSet<Tuple2<String, Double>> topKwithName = topKReducer
				.join(sourceNode).where(1).equalTo(1)
				.flatMap(new ProjectIDWithName());

		topKwithName.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class NodeReader implements
			FlatMapFunction<String, Tuple2<String, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String node = tokens[0];
				long nodeIndex = Long.parseLong(tokens[1]);
				collector.collect(new Tuple2<String, Long>(node, nodeIndex));
			}
		}
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

	public static class ArcReader implements
			FlatMapFunction<String, Tuple2<Long, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<Long, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);
				collector.collect(new Tuple2<Long, Long>(source, target));
			}
		}
	}

	public static class ProjectSourceNodeAndID
			implements
			FlatMapFunction<Tuple2<Tuple1<Long>, Tuple2<String, Long>>, Tuple2<String, Long>> {

		@Override
		public void flatMap(Tuple2<Tuple1<Long>, Tuple2<String, Long>> value,
				Collector<Tuple2<String, Long>> collector) throws Exception {
			String sourceName = value.f1.f0;
			Long id = value.f1.f1;
			collector.collect(new Tuple2<String, Long>(sourceName, id));
		}
	}

	public static class ProjectNameIDAndValues
			implements
			FlatMapFunction<Tuple2<Tuple2<String, Double>, Tuple2<String, Long>>, Tuple3<String, Long, Double>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<String, Double>, Tuple2<String, Long>> value,
				Collector<Tuple3<String, Long, Double>> collector)
				throws Exception {
			collector.collect(new Tuple3<String, Long, Double>(value.f0.f0,
					value.f1.f1, value.f0.f1));
		}
	}

	public static class ProjectArcValues
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple3<String, Long, Double>>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple3<String, Long, Double>> joinTuple,
				Collector<Tuple2<Long, Double>> collector) throws Exception {
			Long id = joinTuple.f0.f0;
			Double value = joinTuple.f1.f2;
			collector.collect(new Tuple2<Long, Double>(id, value));
		}
	}

	public static class TopKMapper implements
			FlatMapFunction<Tuple2<Long, Double>, Tuple3<Long, Long, Double>> {

		@Override
		public void flatMap(Tuple2<Long, Double> tuple,
				Collector<Tuple3<Long, Long, Double>> collector)
				throws Exception {
			collector.collect(new Tuple3<Long, Long, Double>((long) 1,
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
