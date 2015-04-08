/**
 * Track the trackers
 * Copyright (C) 2014  Sebastian Schelter, Felix Neutatz
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

package io.ssc.trackthetrackers.analysis.plots

import java.util

import io.ssc.trackthetrackers.Config
import io.ssc.trackthetrackers.analysis.GraphUtils
import io.ssc.trackthetrackers.analysis.statistics.Dataset
import org.apache.flink.api.java.io.LocalCollectionOutputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.JavaConversions._
import scala.collection.mutable.{BitSet, ListBuffer}

object CompanyCooccurrence extends App {

  compute(Config.get("analysis.trackingraphsample.path"),
    "/home/ssc/Entwicklung/datasets/webdatacommons-hyperlink2012-payleveldomain/pld-index",
    "/home/ssc/Desktop/trackthetrackers/out/companyCooccurrence/")


  def compute(trackingGraphFile: String, domainIndexFile: String, outputPath: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val edges = GraphUtils.readEdges(trackingGraphFile)
    val domains = GraphUtils.readVertices(domainIndexFile)

    val domainsByCompany = Dataset.domainsByCompany
    val companyEdges = edges.filter { edge =>  domainsByCompany.contains(edge.src.toInt) }
                            .map { edge => domainsByCompany(edge.src.toInt) -> edge.target.toInt }
                            .distinct

    val out = new util.ArrayList[(String, Int)]()
    companyEdges.output(new LocalCollectionOutputFormat(out))

    env.execute()

    println("Computing bitsets...")

    val companyBitmaps = Dataset.domainsByCompany.values.toSet
                                .map { company: String => company -> BitSet(Dataset.numPaylevelDomains) }
                                .toMap

    for ((company, seenAt) <- out) {
      companyBitmaps(company).add(seenAt)
    }

    val companies = companyBitmaps.map { case (company, bitset) => company -> bitset.size }
                                  .toArray.sortBy { _._2 }
                                  .map { _._1 }


    val overlaps = ListBuffer[Double]()

    println("Computing overlaps...")

    //TODO intersection computation takes super long, we should switch the underlying bitset implementation
    for (companyA <- companies) {
      val bitmapA = companyBitmaps(companyA)
      for (companyB <- companies) {
        val bitmapB = companyBitmaps(companyB)
        val overlap = bitmapA.intersect(bitmapB).size.toDouble / bitmapB.size
        overlaps += overlap
      }
    }


    println("Plotting results")
    new CorrelationLikeHeatMap("Company Cooccurrences", "CompanyA", "CompanyB", "P(CompanyA|CompanyB)",
                               companies.toArray, overlaps.toArray)

  }

}
