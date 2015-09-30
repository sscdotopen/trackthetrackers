package io.ssc.trackthetrackers.analysis.preprocessing

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object FindTrackersWithoutCompany extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment

  val thirdParties =
    LabeledThirdParties.retrieve("/home/ssc/ownCloud/trackthetrackers/labeling/labeled-thirdparties.csv")


  val candidates =
    thirdParties.filter { _.category.isDefined }
                .filter { t => Set("Advertising", "Analytics", "Widget", "Beacon").contains(t.category.get) }
                .filter { _.company.isEmpty }

  candidates.foreach { c => println(c.domain + " [" + c.category.get + "]") }

}
