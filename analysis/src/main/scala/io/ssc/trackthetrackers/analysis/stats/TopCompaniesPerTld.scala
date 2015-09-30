package io.ssc.trackthetrackers.analysis.stats

import java.io.{BufferedWriter, FileWriter}

import io.ssc.trackthetrackers.analysis.preprocessing.LabeledThirdParties
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object TopCompaniesPerTld extends App {

  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val trackers =
    LabeledThirdParties.retrieve(Constants.datasets("labeled-thirdparties.csv"),
      Set("Advertising", "Analytics", "Widget", "Beacon"))

  val candidateTrackers = trackers.map { _.domain }
                                  .toSet

  val thirdPartyToCompany = trackers.filter { _.company.nonEmpty }
                                    .map { thirdParty => thirdParty.domain -> thirdParty.company.get }
                                    .toMap
  val howMany = 10

  val lines = env.readTextFile(Constants.datasets("thirdparty.tsv"))

  val occurrences = lines.flatMap { line =>
    val tokens = line.split("\t")

    val domain = tokens(0)

    tokens.takeRight(tokens.length - 2).map { token =>
      val parts = token.replaceAll("\\[", "").replaceAll("\\]", "").split(",")

      val thirdParty = parts(0)
      val company = thirdPartyToCompany.getOrElse(thirdParty, "")

      (domain, thirdParty, company, 1)
    }
  }


  val topCompanies = top(occurrences, howMany, "overall") ++
                        top(occurrences.filter { _._1.endsWith(".com") }, howMany, "com") ++
                        top(occurrences.filter { _._1.endsWith(".org") }, howMany, "org") ++
                        top(occurrences.filter { _._1.endsWith(".de") }, howMany, "de") ++
                        top(occurrences.filter { _._1.endsWith(".fr") }, howMany, "fr") ++
                        top(occurrences.filter { _._1.endsWith(".ru") }, howMany, "ru") ++
                        top(occurrences.filter { _._1.endsWith(".cn") }, howMany, "cn") ++
                        top(occurrences.filter { _._1.endsWith(".ir") }, howMany, "ir")


  topCompanies foreach { println }

  var writer: BufferedWriter = null
  try {
    writer = new BufferedWriter(new FileWriter(Constants.output("stats/topcompanies_per_tld.tsv")))

    for (topThirdParty <- topCompanies) {
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

  //TODO validate this
  def top(occurrences: DataSet[(String, String, String, Int)], howMany: Int, label: String) = {

    val t = occurrences.filter { a =>
      candidateTrackers.contains(a._2) && a._3.size > 0
    }

    val count = occurrences.groupBy(0).reduceGroup { _ => 1 -> 1 }
               .groupBy(0).aggregate(Aggregations.SUM, 1).collect.head._2

    val d = t.distinct(0, 2)

    d.map { t => t._3 -> t._4 }.groupBy(0).aggregate(Aggregations.SUM, 1).map { t => t._1 -> (t._2.toDouble / count) }
               .collect().sortBy { _._2 }.takeRight(howMany).reverse.map { t => (label, t._1, t._2) }
  }
}
