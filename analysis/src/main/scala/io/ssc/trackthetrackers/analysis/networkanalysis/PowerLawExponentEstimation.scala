package io.ssc.trackthetrackers.analysis.networkanalysis

import io.ssc.trackthetrackers.analysis.GraphUtils
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * http://konect.uni-koblenz.de/statistics/power
 *
 * Be aware that this code only considers the outdegree distribution currently
 *
 */
@deprecated
object PowerLawExponentEstimation extends App {

  estimatePowerLawExponent("/home/ssc/Entwicklung/projects/trackthetrackers/analysis/src/main/resources/cfindergoogle/links.tsv",
    "/tmp/flink-scala/estimatedExponent")

  def estimatePowerLawExponent(linksFile: String, outputPath: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val edges = GraphUtils.readEdges(linksFile)

    val verticesWithDegree =
      edges.map { edge => edge.src -> 1 }
           .groupBy(0)
           .reduce { (vertexWithCount1, vertexWithCount2) =>
              vertexWithCount1._1 -> (vertexWithCount1._2 + vertexWithCount2._2)
           }

    val minDegree = verticesWithDegree.min(1).map { _._2 }

    val numVertices =
      verticesWithDegree.map { vertexWithDegree => Tuple1(vertexWithDegree._1) }
         .distinct
         .reduceGroup{ _.size }

    val estimatedExponent =
      verticesWithDegree.cross(minDegree) { (vertexWithDegree, minDegree) =>
          math.log(vertexWithDegree._2.toDouble / minDegree)
        }
        .reduce { _ + _ }
        .cross(numVertices) { (v, n) =>

          val gamma = 1.0 + (n / v)
          val sigma = math.sqrt(n) / v

          (gamma, sigma)
        }

    estimatedExponent.writeAsText(outputPath, writeMode = WriteMode.OVERWRITE)

    env.execute()
  }

}
