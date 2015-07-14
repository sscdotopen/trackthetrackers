package io.ssc.trackthetrackers.analysis.networkanalysis

import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


object TopRightVerticesByInDegree extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment


  compute(env, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv",
    "/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "/home/ssc/Desktop/trackthetrackers/out/", 100, 1000)

  compute(env, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph-mil.tsv",
    "/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "/home/ssc/Desktop/trackthetrackers/out/", 100, 0, "mil")

  compute(env, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph-gov.tsv",
    "/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "/home/ssc/Desktop/trackthetrackers/out/", 100, 0, "gov")

  compute(env, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph-br.tsv",
    "/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "/home/ssc/Desktop/trackthetrackers/out/", 100, 1000, "br")

  compute(env, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph-ru.tsv",
    "/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "/home/ssc/Desktop/trackthetrackers/out/", 100, 1000, "ru")

  compute(env, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph-de.tsv",
      "/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "/home/ssc/Desktop/trackthetrackers/out/", 100, 1000, "de")

  compute(env, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph-uk.tsv",
    "/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "/home/ssc/Desktop/trackthetrackers/out/", 100, 1000, "uk")

  compute(env, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph-fr.tsv",
    "/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "/home/ssc/Desktop/trackthetrackers/out/", 100, 1000, "fr")

  compute(env, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph-cn.tsv",
    "/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "/home/ssc/Desktop/trackthetrackers/out/", 100, 1000, "cn")


  def compute(env: ExecutionEnvironment, thirdPartyGraphFile: String, pldIndexFile: String, outputPath: String,
              howMany: Int, threshold: Int, tld: String = "") = {

    val edges = env.readCsvFile[Edge](thirdPartyGraphFile, "\n", "\t")
    val domainNames = env.readCsvFile[(String, Int)](pldIndexFile, "\n", "\t")

    val numVertices = edges.distinct("src").map { _ => Tuple1(1L) }.sum(0).collect().head._1

    val rightDegrees =
      edges.map { edge => (edge.target, 1) }
        .groupBy(0).aggregate(Aggregations.SUM, 1)

    val topRightVerticesWithDegree =
      rightDegrees.filter { vertexWithDegree => vertexWithDegree._2 > threshold }


    val topDomainsWithDegree = topRightVerticesWithDegree.joinWithHuge(domainNames).where(0).equalTo(1) {
      (vertexWithDegree: (Int, Int), vertexWithDomain: (String, Int)) =>
        vertexWithDomain._1 -> (vertexWithDegree._2.toDouble / numVertices)
    }

    val sorted = topDomainsWithDegree.collect().sortBy { case (domain, degree) => degree }.reverse.take(howMany)

    val outputFile = if (tld.isEmpty) {
      outputPath + "domains_with_degrees_" + howMany + ".tsv"
    } else {
      outputPath + "domains_with_degrees_" + howMany + "-" + tld +".tsv"
    }

    FlinkUtils.saveAsCsv(sorted, outputFile)
  }
}
