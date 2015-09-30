package io.ssc.trackthetrackers.analysis.networkanalysis

import java.io.{BufferedWriter, FileWriter}

import io.ssc.trackthetrackers.analysis.preprocessing.LabeledThirdParties
import io.ssc.trackthetrackers.analysis.stats.LogLikelihood
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass
import it.uniroma1.dis.wsngroup.gexf4j.core.{Node, Mode, Graph, EdgeType}
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.{StaxGraphWriter, GexfImpl}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CompanyCooccurrence extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment

  val thirdParties =
    LabeledThirdParties.retrieve("/home/ssc/ownCloud/trackthetrackers/labeling/labeled-thirdparties.csv",
                                 Set("Advertising", "Beacon" ,"Analytics" /*, "Widget"*/))
                       .filter { _.company.isDefined }


  val ds = env.fromCollection(thirdParties)

  val companyOccurrences = ds.map { party => party.company.get -> party.occurrences }
    .groupBy(0).aggregate(Aggregations.SUM, 1)
    .collect().toMap

  val companyNames = thirdParties.map { _.company.get }
                                 .distinct



  val numCompanies = companyNames.size


  // side effect...
  var index = 0
  val companyIndex = companyNames.map { name =>
    val entry = name -> index
    index += 1
    entry
  }
  .toMap

  val domain2CompanyIndex = thirdParties.map { thirdParty => thirdParty.domain -> thirdParty.company.get }
                                        .toMap



  val lines = env.readTextFile("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty.tsv")

  val partialCooccurrences = lines.mapPartition { linesInPartition =>

    val cooccurrences = Array.ofDim[Int](numCompanies, numCompanies)

    linesInPartition.foreach { line =>
      val tokens = line.split("\t")

      val companiesOnDomain = tokens.takeRight(tokens.length - 2)
        .map { token => token.replaceAll("\\[", "").replaceAll("\\]", "").split(",")(0) }
        .flatMap { domain =>
           if (domain2CompanyIndex.contains(domain)) {
             Some(domain2CompanyIndex(domain))
           } else { None }
         }
        .distinct

      if (companiesOnDomain.size > 1) {

        for (companyA <- companiesOnDomain) {
          val indexA = companyIndex(companyA)
          for (companyB <- companiesOnDomain) {
            val indexB = companyIndex(companyB)
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

  val nodes = Array.ofDim[Node](numCompanies)

  for (index <- 0 until numCompanies) {
    val node = graph.createNode(index.toString).setLabel(companyNames(index))
    nodes(index) = node
    val occs = companyOccurrences(companyNames(index))
    node.getAttributeValues.addValue(attLogNumDomains, math.log(occs).toString)
  }
  

  val N: Double = 41192060
  val sampleSizeThreshold = 6
  val tstatisticThreshold = 2.59

  val M = Array.ofDim[Double](cooccurrences.length)
  for (i <- 0 until cooccurrences.length) {
    M(i) = cooccurrences(i).sum
  }

  for (i <- 0 until cooccurrences.length) {

    var llrs = Seq[(Int, Double)]()

    for (j <- i until cooccurrences.length) {

      val M_ij = cooccurrences(i)(j)
      val phi_ij = (N * M_ij - M(i) * M(j)) /
                   math.sqrt(M(i) * M(j) * (N - M(i)) * (N - M(j)))
      
      val t_ij = phi_ij * math.sqrt(math.max(M(i), M(j)) - 2.0) /
                 math.sqrt(1.0 - phi_ij * phi_ij)

      if (M_ij > sampleSizeThreshold && t_ij > tstatisticThreshold) {
        val edge = nodes(i).connectTo(nodes(j))
        edge.setWeight(t_ij.toFloat)
      }

/*
      val k11 = cooccurrences(i)(j)
      val k12 = M(i) - k11
      val k21 = M(j) - k11
      val k22 = N - M(i) - M(j) + k11

      val llr = LogLikelihood.logLikelihoodRatio(k11, k12.toLong, k21.toLong, k22.toLong)

      llrs = llrs ++ Seq(j -> llr)*/
      /*if (llr > 10) {
        nodes(i).connectTo(nodes(j))
      }*/
    }

    /*for (j <- llrs.sortBy(_._2).takeRight(10).map(_._1)) {
      nodes(i).connectTo(nodes(j))
    }*/

  }

  val graphWriter = new StaxGraphWriter()
  graphWriter.writeToStream(gexf, new FileWriter("/home/ssc/Desktop/companies.gexf"), "UTF-8")


  def addArrays(A: Array[Array[Int]], B: Array[Array[Int]]) = {
    for (row <- 0 until B.length) {
      for (column <- 0 until B.length) {
        A(row)(column) += B(row)(column)
      }
    }
    A
  }

}
