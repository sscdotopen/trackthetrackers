package io.ssc.trackthetrackers.analysis.networkanalysis

import java.lang

import io.ssc.trackthetrackers.analysis.{AdjacencyList, AnnotatedVertex, Edge, GraphUtils}
import org.apache.flink.api.common.functions.{RichCoGroupFunction, RichMapFunction}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector


object DanglingPageRank extends App {

  if (args.isEmpty) {
     pageRank("/home/ssc/Desktop/trackthetrackers/test/edges.tsv", "/home/ssc/Desktop/trackthetrackers/test/index.tsv",
      .15, 100, 0.0001, "/tmp/flink-scala/pageRanks/")

  } else {
    pageRank(args(0), args(1), .15, 100, 0.00001, args(2))
  }

  case class RankedVertex(id: Int, rank: Double)
  case class DanglingVertex(id: Int)

  def pageRank(graphFile: String, domainIndexFile: String, teleportationProbability: Double,
               maxIterations: Int, epsilon: Double, outputPath: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val annotatedVertices = GraphUtils.readVertices(domainIndexFile)
    val numVertices = annotatedVertices.map { _ => new Tuple1[Long](1L) }.sum(0).collect().head._1

    val initialRanks =
      annotatedVertices.map { vertex: AnnotatedVertex => RankedVertex(vertex.id, 1.0 / numVertices) }

    val adjacencyLists =
      GraphUtils.readEdges(graphFile).groupBy { _.src }
           .reduceGroup { edges: Iterator[Edge] =>
        val edgeList = edges.toArray

        AdjacencyList(edgeList(0).src, edgeList.map { _.target })
      }

    val danglingVertices =
      initialRanks.coGroup(adjacencyLists).where("id").equalTo("src") {
        (rankedVertices, adjacents, out: Collector[RankedVertex]) =>
          if (adjacents.isEmpty) {
            out.collect(rankedVertices.next)
          }
      }
      .map { x => DanglingVertex(x.id) }

    val ranks = initialRanks.iterateWithTermination(maxIterations) { currentRanks =>

      val danglingRank =
        danglingVertices
          .join(currentRanks).where("id").equalTo("id") { (danglingVertex, rankedVertex) => rankedVertex.rank }
          .reduce { _ + _ }

      val newRanks =
        currentRanks.coGroup(adjacencyLists).where("id").equalTo("src") { new RedistributeRank() }
                    .groupBy("id")
                    .aggregate(Aggregations.SUM, "rank")
                    .map(new RecomputeRank(teleportationProbability, numVertices))
                    .withBroadcastSet(danglingRank, "danglingRank")

      val terminated =
        currentRanks.join(newRanks).where("id").equalTo("id") {
          (currentRank, newRank) => math.abs(currentRank.rank - newRank.rank)
        }
        .reduce { _ + _ }
        .filter { _ >= epsilon }

        (newRanks, terminated)
    }

    ranks.writeAsCsv(outputPath, fieldDelimiter = "\t", writeMode = WriteMode.OVERWRITE)

    env.execute()
  }

  class RedistributeRank() extends RichCoGroupFunction[RankedVertex, AdjacencyList, RankedVertex] {

    override def coGroup(rankedVertices: lang.Iterable[RankedVertex], adjacencyLists: lang.Iterable[AdjacencyList],
                         out: Collector[RankedVertex]): Unit = {

      val rankedVertex = rankedVertices.iterator().next()

      val adjacent = adjacencyLists.iterator();

      if (adjacent.hasNext) {
        val adjacencyList = adjacent.next()
        for (targetId <- adjacencyList.targets) {
          out.collect(RankedVertex(targetId, rankedVertex.rank / adjacencyList.targets.length))
        }
      }

      //fix for vertices with no in-links
      out.collect(RankedVertex(rankedVertex.id, 0.0))
    }
  }


  class RecomputeRank(teleportationProbability: Double, numVertices: Long)
    extends RichMapFunction[RankedVertex, RankedVertex] {

    var danglingRank: Double = _

    override def open(parameters: Configuration): Unit = {
      danglingRank = getRuntimeContext.getBroadcastVariable[Double]("danglingRank").get(0)
    }

    override def map(r: RankedVertex): RankedVertex = {

      val rankFromNeighbors = r.rank

      val newRank =
        (rankFromNeighbors + (danglingRank / numVertices)) * (1.0 - teleportationProbability) +
          (teleportationProbability / numVertices)

      RankedVertex(r.id, newRank)
    }
  }
}
