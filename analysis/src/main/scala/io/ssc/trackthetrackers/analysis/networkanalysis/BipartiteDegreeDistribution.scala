package io.ssc.trackthetrackers.analysis.networkanalysis


import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

//TODO doesn't handle degree zero at the moment
object BipartiteDegreeDistribution extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment

  val edges = env.readCsvFile[Edge]("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv", "\n", "\t")


  val leftDegreesWithFrequencies = degreesWithFrequencies(edges, { edge: Edge => edge.src })
  val rightDegreesWithFrequencies = degreesWithFrequencies(edges, { edge: Edge => edge.target })

  FlinkUtils.saveAsCsv(leftDegreesWithFrequencies, "/home/ssc/Desktop/trackthetrackers/out/left_degrees.tsv")
  FlinkUtils.saveAsCsv(rightDegreesWithFrequencies, "/home/ssc/Desktop/trackthetrackers/out/right_degrees.tsv")

  env.execute()



  def degreesWithFrequencies(edges: DataSet[Edge], extractVertex: Edge => Int) = {
    edges.map { edge => (extractVertex(edge), 1) }
      .groupBy(0).aggregate(Aggregations.SUM, 1)
      .map { vertexWithDegree: (Int, Int) => (vertexWithDegree._2, 1) }
      .groupBy(0).aggregate(Aggregations.SUM, 1)
  }


}
