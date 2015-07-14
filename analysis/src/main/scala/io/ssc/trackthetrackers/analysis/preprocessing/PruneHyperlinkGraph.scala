package io.ssc.trackthetrackers.analysis.preprocessing

import io.ssc.trackthetrackers.Config
import io.ssc.trackthetrackers.analysis.networkanalysis.DanglingPageRank
import DanglingPageRank._
import io.ssc.trackthetrackers.analysis.{Edge, AdjacencyList, AnnotatedVertex, GraphUtils}
import org.apache.flink.api.common.functions.{RichCoGroupFunction, RichMapFunction}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object PruneHyperlinkGraph extends App {

  prune(args(0), args(1), args(2))


  def prune(thirdPartyFile: String, hyperlinkGraphFile: String, outputPath: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    //env.readCsvFile()

  }

}
