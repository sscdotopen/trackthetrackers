package io.ssc.trackthetrackers.analysis.networkanalysis

import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


object TopRightVerticesByWeight extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment


  val edges = env.readCsvFile[Edge]("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv", "\n", "\t")
  val domainNames = env.readCsvFile[(String, Int)]("/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "\n", "\t")
  val weights = env.readCsvFile[(Int, Double)]("/home/ssc/Entwicklung/datasets/thirdparty/pagerank-pld-arc.tsv", "\n", "\t")




  val howMany = 1000

    val weightedLeftVertices = edges.joinWithTiny(weights).where("src").equalTo(0) { (edge: Edge, weight: (Int, Double)) =>
      edge.target -> weight._2
    }
    .groupBy(0).aggregate(Aggregations.SUM, 1)


  val topDomainsWithWeights = weightedLeftVertices.joinWithHuge(domainNames).where(0).equalTo(1) {
    (vertexWithWeight: (Int, Double), vertexWithDomain: (String, Int)) =>
      vertexWithDomain._1 -> vertexWithWeight._2
  }

  val sorted = topDomainsWithWeights.collect().sortBy { case (domain, weight) => weight }.reverse.take(howMany)

  FlinkUtils.saveAsCsv(sorted, "/home/ssc/Desktop/trackthetrackers/out/thirdparties_with_pagerank_" + howMany +".tsv")
}
