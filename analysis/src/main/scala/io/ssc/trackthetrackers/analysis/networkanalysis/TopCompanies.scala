package io.ssc.trackthetrackers.analysis.networkanalysis

import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


object TopCompanies extends App {

  implicit val env = ExecutionEnvironment.getExecutionEnvironment

  val invalidCompanies = Set("Domains By Proxy LLC",  "WHOIS PRIVACY PROTECTION SERVICE INC.", "WHOISGUARD INC.",
                             "c/o whoisproxy.com", "Whois Privacy Protection Service by VALUE-DOMAIN", "_")

  val companiesByDomain = env.readCsvFile[(String, String)]("/home/ssc/Entwicklung/datasets/thirdparty/whois.tsv", "\n", "\t")
                             .filter { domainWithCompany: (String, String) => !invalidCompanies.contains(domainWithCompany._2) }

  val domainNames = env.readCsvFile[(String, Int)]("/home/ssc/Entwicklung/datasets/thirdparty/pld-index", "\n", "\t")

  val domainIdsWithCompanies = domainNames.joinWithTiny(companiesByDomain).where(0).equalTo(0) {
    (domain: (String, Int), domainWithCompany: (String, String)) =>
      domain._2 -> domainWithCompany._2
    }



  val edges = env.readCsvFile[Edge]("/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv", "\n", "\t")

  val companyOccurrences = edges.joinWithTiny(domainIdsWithCompanies).where("target").equalTo(0) { (edge: Edge, domainIdWithCompany: (Int, String)) =>
    edge.src -> domainIdWithCompany._2
  }
    .distinct
    .map { vertexWithCompany: (Int, String) => (vertexWithCompany._2, 1) }


  val companyCounts = companyOccurrences.groupBy(0).aggregate(Aggregations.SUM, 1).collect()

  val sorted = companyCounts.sortBy { case (company, degree) => degree }.reverse

  FlinkUtils.saveAsCsv(sorted, "/home/ssc/Desktop/trackthetrackers/out/top_companies.tsv")
}
