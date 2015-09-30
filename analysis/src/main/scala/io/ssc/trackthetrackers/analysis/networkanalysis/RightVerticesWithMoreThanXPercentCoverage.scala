package io.ssc.trackthetrackers.analysis.networkanalysis

import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object RightVerticesWithMoreThanXPercentCoverage extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment

  compute(env, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv",
    "/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "/home/ssc/Desktop/trackthetrackers/out/", 0.01)

  def compute(env: ExecutionEnvironment, thirdPartyGraphFile: String, pldIndexFile: String, outputPath: String,
              thresholdInPercent: Double, tld: String = "") = {

    val edges = env.readCsvFile[Edge](thirdPartyGraphFile, "\n", "\t")
    val domainNames = env.readCsvFile[(String, Int)](pldIndexFile, "\n", "\t")

    val numVertices = edges.distinct("src").map { _ => Tuple1(1L) }.sum(0).collect().head._1

    val threshold = 10000//(thresholdInPercent * 0.01 * numVertices).toLong

    val rightDegrees =
      edges.map { edge => (edge.target, 1) }
        .groupBy(0).aggregate(Aggregations.SUM, 1)

    val topRightVerticesWithDegree =
      rightDegrees.filter { vertexWithDegree => vertexWithDegree._2 > threshold }


    val topDomainsWithDegree = topRightVerticesWithDegree.joinWithHuge(domainNames).where(0).equalTo(1) {
      (vertexWithDegree: (Int, Int), vertexWithDomain: (String, Int)) =>
        vertexWithDomain._1 -> (vertexWithDegree._2.toDouble / numVertices)
    }

    val sorted = topDomainsWithDegree.collect().sortBy { case (domain, degree) => degree }.reverse

    val outputFile = if (tld.isEmpty) {
      outputPath + "thirdparties_with_more_than_" + threshold + ".tsv"
    } else {
      outputPath + "thirdparties_with_more_than_" + thresholdInPercent + "_percent-" + tld +".tsv"
    }

    FlinkUtils.saveAsCsv(sorted, outputFile)
  }
}
