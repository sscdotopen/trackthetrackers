package io.ssc.trackthetrackers.analysis.preprocessing

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

case class RawLabeledThirdParty(domain: String, registrationOrg: String, registrationCountry: String, occurrences: Int,
                                occurrencesDynamic: Int, occurrencesStatic: Int,
                                occurrencesJavascript: Int, occurrencesIframe: Int, occurrencesImage: Int,
                                occurrencesLink: Int, category: String, comment: String, company: String, found: String,
                                certain: String, labelingComment: String, requiresTranslation: String) {

  def asLabeledThirdParty() = {
    LabeledThirdParty(domain, occurrences, occurrencesDynamic, occurrencesStatic, occurrencesJavascript,
                      occurrencesIframe, occurrencesImage, occurrencesLink, optionize(category),
                      optionize(registrationOrg), optionize(company))

  }

  def optionize(value: String): Option[String] = {
    value match {
      case "#" => None
      case _ => Some(value)
    }
  }
}

case class LabeledThirdParty(domain: String, occurrences: Int, occurrencesDynamic: Int, occurrencesStatic: Int,
                             occurrencesJavascript: Int, occurrencesIframe: Int, occurrencesImage: Int,
                             occurrencesLink: Int, category: Option[String], registrationOrg: Option[String],
                             company: Option[String])

object LabeledThirdParties {

  def retrieve(file: String, categories: Set[String] = Set())(implicit env: ExecutionEnvironment) = {
    val labeledThirdParties = env.readCsvFile[RawLabeledThirdParty](file, fieldDelimiter = "\t")
                                 .map { _.asLabeledThirdParty() }
                                 .filter { labeledThirdParty =>
                                    if (categories.isEmpty) { true } else {
                                      labeledThirdParty.category.exists { categories.contains(_) }
                                    }
                                  }
                                 .collect()

    labeledThirdParties.sortBy { _.occurrences }
                       .reverse
  }

}


object PlayWithLabeledThirdParties extends App {

  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val parties = LabeledThirdParties.retrieve("/home/ssc/ownCloud/trackthetrackers/labeling/labeled-thirdparties.csv",
    Set("Advertising", "Analytics", "Beacon", "Widget"))

  parties.filter { party => party.company.isEmpty && party.registrationOrg.isDefined  }
         .foreach { party => println(party.domain + " " + party.registrationOrg.get) }
}


































