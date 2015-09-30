package io.ssc.trackthetrackers.analysis.preprocessing

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

case class ThirdPartyStat(val name: String, val domains: Int, val scripts: Int, val iframes: Int, val images: Int, val links: Int) {
  def merge(other: ThirdPartyStat) = {
    ThirdPartyStat(name, domains + other.domains, scripts + other.scripts, iframes + other.iframes, images + other.images, links + other.links)
  }

}

object ThirdPartyStats extends App {

  val threshold = 2500

  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val lines = env.readTextFile("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty.tsv")

  val entries = lines.flatMap { line =>
    val tokens = line.split("\t")

    tokens.takeRight(tokens.length - 2).map { token =>
      val parts = token.replaceAll("\\[", "").replaceAll("\\]", "").split(",")

      ThirdPartyStat(parts(0), 1, binarize(parts(1).toInt), binarize(parts(2).toInt), binarize(parts(3).toInt), binarize(parts(4).toInt))
    }

  }

  val stats = entries.groupBy("name").reduce(_.merge(_)).filter { _.domains >= threshold }

  val sorted = stats.collect().sortBy { _.domains }.reverse

  sorted.map { stat => stat.name + ";" + stat.domains + ";" + stat.scripts + ";" + stat.iframes + ";" + stat.images + ";" + stat.links }
        .foreach { println }

  //env.execute()

  def binarize(num: Int) = if (num > 0) { 1 } else { 0 }

}
