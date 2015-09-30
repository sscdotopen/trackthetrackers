package io.ssc.trackthetrackers.analysis.stats

import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object TopThirdPartiesByPageRank extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment


  val edges = env.readCsvFile[Edge](Constants.datasets("thirdparty-graph.tsv"), "\n", "\t")
  val domainNames = env.readCsvFile[(String, Int)](Constants.datasets("pld-index"), "\n", "\t")
  val ranks = env.readCsvFile[(Int, Double)](Constants.datasets("pagerank-pld-arc.tsv"), "\n", "\t")
 

  val howMany = 1000

    val pagesWithRanks = edges.joinWithTiny(ranks).where("src").equalTo(0) { (edge: Edge, weight: (Int, Double)) =>
      edge.target -> weight._2
    }
    .groupBy(0).aggregate(Aggregations.SUM, 1)


  val topThirdPartiesWithRanks = pagesWithRanks.joinWithHuge(domainNames).where(0).equalTo(1) {
    (vertexWithWeight: (Int, Double), vertexWithDomain: (String, Int)) =>
      vertexWithDomain._1 -> vertexWithWeight._2
  }

  val sorted = topThirdPartiesWithRanks.collect().sortBy { case (domain, weight) => weight }.reverse.take(howMany)

  FlinkUtils.saveAsCsv(sorted, Constants.output("thirdparties_with_pagerank_" + howMany +".tsv"))
}
