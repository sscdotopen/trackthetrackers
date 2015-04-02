/**
 * Track the trackers
 * Copyright (C) 2014  Sebastian Schelter
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
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object TrackerDistribution extends App {

  computeDistribution(Config.get("analysis.trackingraphsample.path"), Config.get("webdatacommons.pldfile.unzipped"),
      Config.get("analysis.results.path") + "trackerDistribution")

  def computeDistribution(trackingGraphFile: String, domainIndexFile: String, outputPath: String) = {

    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val edges = GraphUtils.readEdges(trackingGraphFile)
    val domains = GraphUtils.readVertices(domainIndexFile)

    val numTrackedHosts = edges.distinct("target").map { _ => Tuple1(1L) }.sum(0)


    val trackersWithNumDomainsTracked = FlinkUtils.countByKey(edges, { edge: Edge => edge.src })

    val topTrackers = trackersWithNumDomainsTracked.map(new TrackerProbability())
                                                   .withBroadcastSet(numTrackedHosts, "numTrackedHosts")
                                                   .filter { _._2 >= 0.005 }
    
    val topTrackerDomains =
        topTrackers.join(domains).where(0).equalTo(1) { (topTracker, domain) => domain.annotation -> topTracker._2 }
    
    topTrackerDomains.writeAsCsv(outputPath, fieldDelimiter = "\t", writeMode = WriteMode.OVERWRITE)

    env.execute()
  }


  class TrackerProbability() extends RichMapFunction[(Int, Long), (Int, Double)] {

    override def map(trackedIdWithCount: (Int, Long)): (Int, Double) = {

      val numTrackedHosts = getRuntimeContext.getBroadcastVariable[Tuple1[Long]]("numTrackedHosts").get(0)._1

      trackedIdWithCount._1.toInt -> trackedIdWithCount._2.toDouble / numTrackedHosts
    }
  }

}
