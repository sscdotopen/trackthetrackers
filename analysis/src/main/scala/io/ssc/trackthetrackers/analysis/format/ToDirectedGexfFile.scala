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

package io.ssc.trackthetrackers.analysis.format

import java.io.{BufferedWriter, File, FileWriter}
import java.util

import io.ssc.trackthetrackers.Config
import io.ssc.trackthetrackers.analysis._
import io.ssc.trackthetrackers.analysis.statistics.Dataset
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.java.io.LocalCollectionOutputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.com.google.common.io.Closeables

import scala.collection.JavaConversions._

object ToDirectedGexfFile extends App {
/*
  compute(Config.get("analysis.trackingraphminisample.path"), Config.get("webdatacommons.pldarcfile.unzipped"),
    Config.get("webdatacommons.pldfile.unzipped"), Config.get("analysis.results.path") + "sample.gexf")

  def compute(trackingGraphFile: String, hyperlinkGraphFile: String, domainIndexFile: String,
              gexfOutputFile: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val hyperlinkGraphEdges = GraphUtils.readEdges(hyperlinkGraphFile)
    val trackingGraphEdges = GraphUtils.readEdges(trackingGraphFile)
    val domains = GraphUtils.readVertices(domainIndexFile)

    val trackingGraphVertices = trackingGraphEdges.map { edge => Tuple1(edge.target) }
                                                  .distinct

    val containedDomains = trackingGraphVertices.join(domains).where(0).equalTo("id") { (_, annotatedVertex) =>
      annotatedVertex.id -> annotatedVertex.annotation
    }

    val companyEdges = trackingGraphEdges.filter { edge => Dataset.domainsByCompany.contains(edge.src) }
                                         .map { edge => edge.target -> Dataset.domainsByCompany(edge.src) }
                                         .distinct

    val verticesWithDomainAndTrackingCompanies = containedDomains.coGroup(companyEdges).where(0).equalTo(0) {
      (vertices: Iterator[(Int, String)], trackingCompanies: Iterator[(Int, String)]) =>

        val (id, domain) = vertices.toSeq.get(0)

        val companies = trackingCompanies.map { case (id, company) => company }
                                         .toSeq

        (id, domain, companies)
    }

    val prunedHyperlinkGraph = hyperlinkGraphEdges.filter(new PruneToSampleVerticesFilter)
                                                  .withBroadcastSet(trackingGraphVertices, "vertices")


    val nodes = new util.ArrayList[(Int, String, Seq[String])]()
    verticesWithDomainAndTrackingCompanies.output(new LocalCollectionOutputFormat(nodes))

    val edges = new util.ArrayList[Edge]()
    prunedHyperlinkGraph.output(new LocalCollectionOutputFormat(edges))

    env.execute()

    var writer: BufferedWriter = null

    try {
      writer = new BufferedWriter(new FileWriter(new File(gexfOutputFile)))
      
      writer.write(
        """
          |<gexf xmlns="http://www.gexf.net/1.2draft" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd" version="1.2">\n
          |  <meta lastmodifieddate="2009-03-20">
          |    <creator>Track the Trackers</creator>
          |    <description>Tracking on the Web</description>
          |  </meta>\n
          |  <graph defaultedgetype="directed">
          |    <attributes class="node">
          |      <attribute id="0" title="trackableByAddThis" type="boolean"/>
          |      <attribute id="1" title="trackableByAmazon" type="boolean"/>
          |      <attribute id="2" title="trackableByCasaleMedia" type="boolean"/>
          |      <attribute id="3" title="trackableByFacebook" type="boolean"/>
          |      <attribute id="4" title="trackableByGoogle" type="boolean"/>
          |      <attribute id="5" title="trackableByPhotobucket" type="boolean"/>
          |      <attribute id="6" title="trackableByStatCounter" type="boolean"/>
          |      <attribute id="7" title="trackableByTwitter" type="boolean"/>
          |      <attribute id="8" title="trackableByYahoo" type="boolean"/>
          |    </attributes>
          |    <nodes>""".stripMargin)

      for ((id, domain, companies) <- nodes) {
        writer.write(
          """
            |      <node id="%s" label="%s">
            |        <attvalues>
            |          <attvalue for="0" value="%s"/>
            |          <attvalue for="1" value="%s"/>
            |          <attvalue for="2" value="%s"/>
            |          <attvalue for="3" value="%s"/>
            |          <attvalue for="4" value="%s"/>
            |          <attvalue for="5" value="%s"/>
            |          <attvalue for="6" value="%s"/>
            |          <attvalue for="7" value="%s"/>
            |          <attvalue for="8" value="%s"/>
            |        </attvalues>
            |      </node>""".stripMargin.format(id, domain, companies.contains("AddThis"),
                                                             companies.contains("Amazon"),
                                                             companies.contains("CasaleMedia"),
                                                             companies.contains("Facebook"),
                                                             companies.contains("Google"),
                                                             companies.contains("Photobucket"),
                                                             companies.contains("StatCounter"),
                                                             companies.contains("Twitter"),
                                                             companies.contains("Yahoo")))
      }

      writer.write("    </nodes>\n")
      writer.write("    <edges>\n")

      for (edge <- edges) {
        writer.write("      <edge source=\"%s\" target=\"%s\"/>\n".format(edge.src, edge.target))
      }

      writer.write(
        """
          |    </edges>
          |  </graph>
          |</gexf>
        """.stripMargin)


    } finally {
      Closeables.close(writer, false)
    }
  }
  
  class PruneToSampleVerticesFilter extends RichFilterFunction[Edge] {

    var vertices: Set[Int] = null
    
    override def open(parameters: Configuration) = {
      vertices = getRuntimeContext.getBroadcastVariable[Tuple1[Int]]("vertices").map { _._1 }
                                                                                .toSet
    }
    
    override def filter(edge: Edge): Boolean = {
      vertices.contains(edge.src) && vertices.contains(edge.target)
    }
  }
*/
}
