package io.ssc.trackthetrackers.analysis.networkanalysis

import java.io.{BufferedWriter, FileWriter}

import io.ssc.trackthetrackers.analysis.preprocessing.LabeledThirdParties
import io.ssc.trackthetrackers.analysis.stats.LogLikelihood
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass
import it.uniroma1.dis.wsngroup.gexf4j.core.{Node, Mode, Graph, EdgeType}
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.{StaxGraphWriter, GexfImpl}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;

import scala.collection.mutable

object TrackerCooccurrence extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment

  val thirdParties = 
    LabeledThirdParties.retrieve("/home/ssc/ownCloud/trackthetrackers/labeling/labeled-thirdparties.csv", 
                                 Set("Advertising", "Beacon"))
  
  val numThirdParties = thirdParties.size
  val thirdPartyNames = thirdParties.map { _.domain }
                                    .toSet

  val thirdPartyIndex = mutable.Map[String, Int]()
  for (index <- 0 until numThirdParties) {
    thirdPartyIndex.put(thirdParties(index).domain, index)
  }
  
  
  val lines = env.readTextFile("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty.tsv")

  val partialCooccurrences = lines.mapPartition { linesInPartition =>

    val cooccurrences = Array.ofDim[Int](numThirdParties, numThirdParties)

    linesInPartition.foreach { line =>
      val tokens = line.split("\t")

      val trackersOnDomain = tokens.takeRight(tokens.length - 2)
        .map { token => token.replaceAll("\\[", "").replaceAll("\\]", "").split(",")(0) }
        .filter { tracker => thirdPartyNames.contains(tracker) }

      if (trackersOnDomain.size > 1) {

        for (trackerA <- trackersOnDomain) {
          val indexA = thirdPartyIndex(trackerA)
          for (trackerB <- trackersOnDomain) {
            val indexB = thirdPartyIndex(trackerB)
            cooccurrences(indexA)(indexB) += 1
          }
        }
      }
    }

    Iterator.single(cooccurrences)
  }

  val cooccurrences = partialCooccurrences.reduce { (A: Array[Array[Int]], B: Array[Array[Int]]) => addArrays(A, B) }
                                          .collect()
                                          .head

  val gexf = new GexfImpl()
  val graph = gexf.getGraph()
  graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC)
  val attrList = new AttributeListImpl(AttributeClass.NODE)
  graph.getAttributeLists().add(attrList)

  val attLogNumDomains = attrList.createAttribute("0", AttributeType.DOUBLE, "logNumDomains")

  val nodes = Array.ofDim[Node](numThirdParties)

  for (index <- 0 until numThirdParties) {
    val node = graph.createNode(index.toString).setLabel(thirdParties(index).domain)
    nodes(index) = node
    node.getAttributeValues.addValue(attLogNumDomains, math.log(thirdParties(index).occurrences).toString)
  }

  val gexf2 = new GexfImpl()
  val graph2 = gexf2.getGraph()
  graph2.setDefaultEdgeType(EdgeType.DIRECTED).setMode(Mode.STATIC)
  val attrList2 = new AttributeListImpl(AttributeClass.NODE)
  graph2.getAttributeLists().add(attrList)

  val attLogNumDomains2 = attrList2.createAttribute("0", AttributeType.DOUBLE, "logNumDomains")

  val nodes2 = Array.ofDim[Node](numThirdParties)

  for (index <- 0 until numThirdParties) {
    val node2 = graph2.createNode(index.toString).setLabel(thirdParties(index).domain)
    nodes2(index) = node2
    node2.getAttributeValues.addValue(attLogNumDomains2, math.log(thirdParties(index).occurrences).toString)
  }

  var edges = 0


  val conditionalProbabilities = Array.ofDim[Double](numThirdParties, numThirdParties)

  for (row <- 0 until cooccurrences.length) {
    for (column <- 0 until cooccurrences.length) {

      val condProb = cooccurrences(row)(column).toDouble / cooccurrences(column)(column).toDouble

      val probRow = cooccurrences(row)(row).toDouble / 41192060

      conditionalProbabilities(row)(column) = condProb / probRow
    }
  }


  val condProbThreshold = 0.15

  for (row <- 0 until cooccurrences.length) {
    for (column <- 0 until cooccurrences.length) {
      if (row != column && cooccurrences(row)(column) > 0) {

        val condProb = cooccurrences(row)(column).toDouble / cooccurrences(column)(column).toDouble

        if (condProb > condProbThreshold) {
          val edge2 = nodes2(row).connectTo(nodes2(column))
          edge2.setWeight(condProb.toFloat)
          edge2.setEdgeType(EdgeType.DIRECTED)

        }
      }
    }
  }

  val N: Double = 41192060
  val sampleSizeThreshold = 6
  val tstatisticThreshold = 2.59

  val M = Array.ofDim[Double](cooccurrences.length)
  for (i <- 0 until cooccurrences.length) {
    M(i) = cooccurrences(i).sum
  }



  for (i <- 0 until cooccurrences.length) {
    for (j <- i until cooccurrences.length) {

      /*
      val M_ij = cooccurrences(i)(j)
      val phi_ij = (N * M_ij - M(i) * M(j)).toDouble /
                   math.sqrt(M(i) * M(j) * (N - M(i)) * (N - M(j)))
      
      val t_ij = phi_ij * math.sqrt(math.max(M(i), M(j)) - 2.0) /
                 math.sqrt(1.0 - phi_ij * phi_ij)

      if (M_ij > sampleSizeThreshold && t_ij > tstatisticThreshold) {
        val edge = nodes(i).connectTo(nodes(j))
        edge.setWeight(t_ij.toFloat)
      }*/

      val k11 = cooccurrences(i)(j)
      val k12 = M(i) - k11
      val k21 = M(j) - k11
      val k22 = N - M(i) - M(j) + k11

      val llr = LogLikelihood.rootLogLikelihoodRatio(k11, k12.toLong, k21.toLong, k22.toLong)


      if (llr > 15) {
        val edge = nodes(i).connectTo(nodes(j))
      }
    }


  }

  val graphWriter = new StaxGraphWriter()
  graphWriter.writeToStream(gexf, new FileWriter("/home/ssc/Desktop/cooc-llr.gexf"), "UTF-8")
  graphWriter.writeToStream(gexf2, new FileWriter("/home/ssc/Desktop/cooc2.gexf"), "UTF-8")

  var writer: BufferedWriter = null
  try {
    writer = new BufferedWriter(new FileWriter("/home/ssc/Desktop/trackthetrackers/out/stats/thirdparty_conditionalprobabilities.tsv"))

    for (i <- 0 until cooccurrences.length) {
      for (j <- 0 until cooccurrences.length) {
        writer.write(i.toString)
        writer.write("\t")
        writer.write(j.toString)
        writer.write("\t")
        writer.write(conditionalProbabilities(i)(j).toString)
        writer.newLine()
      }
    }



  } finally {
    writer.close()
  }


  def addArrays(A: Array[Array[Int]], B: Array[Array[Int]]) = {
    for (row <- 0 until B.length) {
      for (column <- 0 until B.length) {
        A(row)(column) += B(row)(column)
      }
    }
    A
  }

}
