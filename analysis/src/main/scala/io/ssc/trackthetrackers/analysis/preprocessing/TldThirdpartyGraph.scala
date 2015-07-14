package io.ssc.trackthetrackers.analysis.preprocessing

import io.ssc.trackthetrackers.analysis.{FlinkUtils, Edge}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object TldThirdpartyGraph extends App {

  val tld = "gov"

  implicit val env = ExecutionEnvironment.getExecutionEnvironment

  val edges = env.readCsvFile[Edge]("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv", "\n", "\t")
  val domainNames = env.readCsvFile[(String, Int)]("/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "\n", "\t")

  val deDomains = domainNames.filter { domainWithId: (String, Int) => domainWithId._1.endsWith("." + tld) }

  val deEdges = edges.joinWithTiny(deDomains).where("src").equalTo(1) { (edge: Edge, _) => edge }

  val out = deEdges.map { edge: Edge => edge.src -> edge.target }

  FlinkUtils.saveAsCsv(out, "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph-" + tld + ".tsv")

  env.execute()
}
