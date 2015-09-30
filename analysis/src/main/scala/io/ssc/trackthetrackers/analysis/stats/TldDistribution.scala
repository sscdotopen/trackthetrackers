package io.ssc.trackthetrackers.analysis.stats

import io.ssc.trackthetrackers.analysis.FlinkUtils
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object TldDistribution extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment

  val lines = env.readTextFile(Constants.datasets("thirdparty.tsv"))

  val counts = lines.map { line => line.split("\t").head.split("\\.").last -> 1 }
       .groupBy(0)
       .aggregate(Aggregations.SUM, 1)

  val sortedCounts = counts.collect.sortBy { case (tld, count) => count }.reverse

  FlinkUtils.saveAsCsv(sortedCounts, Constants.output("stats/topleveldomain_distribution.tsv"))
}
