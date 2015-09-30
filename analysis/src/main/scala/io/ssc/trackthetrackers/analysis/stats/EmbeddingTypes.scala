package io.ssc.trackthetrackers.analysis.stats

import io.ssc.trackthetrackers.analysis.FlinkUtils
import io.ssc.trackthetrackers.analysis.preprocessing.LabeledThirdParties
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object EmbeddingTypes extends App {

  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val file = "/home/ssc/ownCloud/trackthetrackers/labeling/labeled-thirdparties.csv"

  val embeddingStats =
    Array(Set[String](), Set("Advertising"), Set("Analytics"), Set("Beacon"), Set("Widget"), Set("ImageHoster"), Set("ContentDelivery")).map { categories =>
      ratios(file, categories)
    }

  embeddingStats foreach { println }

  //TODO check!
  val stats = env.fromCollection(embeddingStats).map { a => a }

  stats.collect()

  FlinkUtils.saveAsCsv(stats, Constants.output("stats/embedding_types.tsv"))

  def ratios(file: String, categories: Set[String])(implicit env: ExecutionEnvironment) = {

    val thirdParties = LabeledThirdParties.retrieve(file, categories)

    var dynamicRatioSum = 0.0
    var staticRatioSum = 0.0

    val dynamicRatioStats = new RunningAverageAndStdDev()
    val staticRatioStats = new RunningAverageAndStdDev()

    for (thirdParty <- thirdParties) {
      val dynamicRatio = thirdParty.occurrencesDynamic.toDouble / thirdParty.occurrences
      val staticRatio = thirdParty.occurrencesStatic.toDouble / thirdParty.occurrences

      dynamicRatioStats.addDatum(dynamicRatio)
      staticRatioStats.addDatum(staticRatio)
    }

    val label = if (categories.isEmpty) { "Overall" } else { categories.mkString(",") }

    (label, dynamicRatioStats.getAverage, dynamicRatioStats.getStandardDeviation, staticRatioStats.getAverage,
            staticRatioStats.getStandardDeviation)
  }

}
