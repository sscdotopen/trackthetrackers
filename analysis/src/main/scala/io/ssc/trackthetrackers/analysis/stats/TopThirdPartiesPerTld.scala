package io.ssc.trackthetrackers.analysis.stats


import java.io.{FileWriter, BufferedWriter}

import io.ssc.trackthetrackers.analysis.preprocessing.LabeledThirdParties
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object TopThirdPartiesPerTld extends App {

  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val trackers =
    LabeledThirdParties.retrieve(Constants.datasets("labeled-thirdparties.csv"),
      Set("Advertising", "Analytics", "Widget", "Beacon"))

  val candidateTrackers = trackers.map { _.domain }
                                  .toSet

  val howMany = 20

  val lines = env.readTextFile(Constants.datasets("thirdparty.tsv"))

  val occurrences = lines.flatMap { line =>
    val tokens = line.split("\t")

    val domain = tokens(0)

    tokens.takeRight(tokens.length - 2).map { token =>
      val parts = token.replaceAll("\\[", "").replaceAll("\\]", "").split(",")

      (domain, parts(0), 1)
    }
  }


  val topThirdParties = top(occurrences, howMany, "overall") ++
                        top(occurrences.filter { _._1.endsWith(".com") }, howMany, "com") ++
                        top(occurrences.filter { _._1.endsWith(".org") }, howMany, "org") ++
                        top(occurrences.filter { _._1.endsWith(".de") }, howMany, "de") ++
                        top(occurrences.filter { _._1.endsWith(".fr") }, howMany, "fr") ++
                        top(occurrences.filter { _._1.endsWith(".ru") }, howMany, "ru") ++
                        top(occurrences.filter { _._1.endsWith(".cn") }, howMany, "cn") ++
                        top(occurrences.filter { _._1.endsWith(".ir") }, howMany, "ir")


  topThirdParties foreach { println }

  var writer: BufferedWriter = null
  try {
    writer = new BufferedWriter(new FileWriter(Constants.output("stats/topthirdparties_per_tld.tsv")))

    for (topThirdParty <- topThirdParties) {
      writer.write(topThirdParty._1)
      writer.write("\t")
      writer.write(topThirdParty._2)
      writer.write("\t")
      writer.write(topThirdParty._3.toString)
      writer.newLine()
    }

  } finally {
    writer.close()
  }


  def top(occurrences: DataSet[(String, String, Int)], howMany: Int, label: String) = {

    val t = occurrences.filter { a =>
      candidateTrackers.contains(a._2)
    }

    val count = occurrences.groupBy(0).reduceGroup { _ => 1 -> 1 }
               .groupBy(0).aggregate(Aggregations.SUM, 1).collect.head._2

    t.map { t => t._2 -> t._3 }.groupBy(0).aggregate(Aggregations.SUM, 1).map { t => t._1 -> (t._2.toDouble / count) }
               .collect().sortBy { _._2 }.takeRight(howMany).reverse.map { t => (label, t._1, t._2) }
  }
}
