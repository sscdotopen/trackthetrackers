package io.ssc.trackthetrackers.analysis.preprocessing

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

case class EmbeddingStat(val name: String, val domains: Int, val dynamic: Int, val static: Int) {
  def merge(other: EmbeddingStat) = {
    EmbeddingStat(name, domains + other.domains, dynamic + other.dynamic, static + other.static)
  }

}

object TopThirdPartyEmbeddingType extends App {

  val threshold = 2500

  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val lines = env.readTextFile("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty.tsv")

  val entries = lines.flatMap { line =>
    val tokens = line.split("\t")

    tokens.takeRight(tokens.length - 2).map { token =>
      val parts = token.replaceAll("\\[", "").replaceAll("\\]", "").split(",")

      EmbeddingStat(parts(0), 1, binarize(parts(1).toInt, parts(2).toInt), binarize(parts(3).toInt, parts(4).toInt))
    }

  }

  val stats = entries.groupBy("name").reduce(_.merge(_)).filter { _.domains >= threshold }

  val sorted = stats.collect().sortBy { _.domains }.reverse

  sorted.map { stat => stat.name + ";" + stat.domains + ";" + stat.dynamic + ";" + stat.static }
        .foreach { println }

  //env.execute()

  def binarize(num1: Int, num2: Int) = if (num1 > 0 || num2 > 0) { 1 } else { 0 }

}
