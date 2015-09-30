package io.ssc.trackthetrackers.analysis.preprocessing

import java.io.{FileWriter, BufferedWriter}

import org.apache.commons.net.whois.WhoisClient
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.sys.process._

object WhoisInfo extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment

  val thirdParties =
    LabeledThirdParties.retrieve("/home/ssc/ownCloud/trackthetrackers/labeling/labeled-thirdparties.csv")

  var count = 0

  val domains = thirdParties.map { _.domain }

  var writer: BufferedWriter = null
  try {
    writer = new BufferedWriter(new FileWriter("/home/ssc/Desktop/trackthetrackers/whois.tsv"))



    for (domain <- domains) {
      val output = try {
        "whois " + domain !!
      } catch {
        case _ => ""
      }

      val result = resultToRecords(output)

      println(domain + "\t" + result.getOrElse("Registrant Organization", "#") + "\t" + result.getOrElse("Registrant Country", "#"))

      writer.write(domain)
      writer.write("\t")
      writer.write(result.getOrElse("Registrant Organization", "#"))
      writer.write("\t")
      writer.write(result.getOrElse("Registrant Country", "#"))
      writer.newLine()

      Thread.sleep(1000)
    }

  } finally {
    writer.close()
  }

  def resultToRecords(result: String): Map[String, String] = {
    result.split("\n").filter { line => line.contains(":") }
          .map { line =>
                 val parts = line.split(":", 2)
                 parts(0).trim -> parts(1).trim
               }
          .toMap
  }

}
