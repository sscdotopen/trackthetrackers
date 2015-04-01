/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.analysis.statistics

import io.ssc.trackthetrackers.Config
import io.ssc.trackthetrackers.analysis.{Edge, FlinkUtils, GraphUtils}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

object CompanyDistribution extends App {

  case class Rankvertex(vertex: String, rank: Double)

  computeDistribution(Config.get("analysis.trackingraphsample.path"), Config.get("webdatacommons.pldfile.unzipped"),
    Config.get("analysis.results.path") + "companyDistribution", null, Config.get("webdatacommons.hostgraph-pr.unzipped"), 3000)

  def computeDistribution(trackingGraphFile: String, domainIndexFile: String, outputPath: String, toplevelDomain: String, pageRankFile: String, topKdomains: Int) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val edges = GraphUtils.readEdges(trackingGraphFile)

    val domains = GraphUtils.readVertices(domainIndexFile)
    
    //filter by pageRank
    var companyEdgesByPageRank = edges
    if (pageRankFile != null && topKdomains > 0) {
      val pageRankVertices = env.readCsvFile[Rankvertex](pageRankFile, "\n", "\t").map(rank => (rank.vertex, rank.rank))

      val result = pageRankVertices.join(domains)
        .where(0)
        .equalTo(0) 
          { (pageRankVertex, annotatedVertex) => (0, pageRankVertex._1, pageRankVertex._2, annotatedVertex.id)} //url => id
        .groupBy(0)
        .sortGroup(2, Order.DESCENDING) //sort by PageRank
        .first(topKdomains) //filter first k domains
        //.map { t => (t._4, t._3) } //map to -> (domainID, pageRank)
        
      companyEdgesByPageRank = edges.join(result).where("target").equalTo(3)
                                          { (edge, rankVertex) => edge }
      
    }
    
    
    //filter by domain
    var companyEdgesByTopleveldomain = companyEdgesByPageRank
    if ( toplevelDomain != null) {
      val companyDomains =
        edges.join(domains).where(1).equalTo(1) { (edge, domain) => (edge.src, edge.target, domain.annotation)}

      companyEdgesByTopleveldomain = companyDomains.filter(domain => domain._3.endsWith(toplevelDomain))
        .map { tuple => new Edge(tuple._1, tuple._2)}
    }

    val numTrackedHosts = companyEdgesByTopleveldomain.distinct("target").map { _ => Tuple1(1L) }.sum(0)

    val companyEdges = companyEdgesByTopleveldomain.filter { edge => Dataset.domainsByCompany.contains(edge.src.toInt) }
                            .map { edge => Dataset.domainsByCompany(edge.src.toInt) -> edge.target }
                            .distinct   

    val companyCounts = FlinkUtils.countByStrKey(companyEdges, { t: (String, Int) => t._1 })

    val companyProbabilities = companyCounts.map(new CompanyProbability())
                                            .withBroadcastSet(numTrackedHosts, "numTrackedHosts")

    companyProbabilities.writeAsCsv(outputPath, fieldDelimiter = "\t", writeMode = WriteMode.OVERWRITE)
    env.execute()
  }

  class CompanyProbability() extends RichMapFunction[(String, Long), (String, Double)] {

    override def map(companyWithCount: (String, Long)): (String, Double) = {

      val numTrackedHosts = getRuntimeContext.getBroadcastVariable[Tuple1[Long]]("numTrackedHosts").get(0)._1

      companyWithCount._1 -> companyWithCount._2.toDouble / numTrackedHosts
    }
  }

}
