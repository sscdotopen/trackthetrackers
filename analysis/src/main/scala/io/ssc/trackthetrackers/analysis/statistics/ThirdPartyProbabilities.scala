package io.ssc.trackthetrackers.analysis.statistics

import io.ssc.trackthetrackers.Config
import io.ssc.trackthetrackers.analysis.{FlinkUtils, ThirdParty}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object ThirdPartyProbabilities extends App {

  compute(Config.get("analysis.trackingraph.path"), Config.get("analysis.results.path") + "thirdPartyProbabilities")

  def compute(inputFile: String, outputPath: String) = {
    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val thirdParties = env.readCsvFile[ThirdParty](inputFile, "\n", "\t")

    val numTrackedHosts = thirdParties.distinct("hostDomainIndex").map { _ => Tuple1(1L) }.sum(0)

    val trackersWithNumDomainsTracked =
      FlinkUtils.countByStrKey(thirdParties, { thirdParty: ThirdParty => thirdParty.thirdPartyDomain })

    val topTrackers = trackersWithNumDomainsTracked.map(new TrackerProbability())
      .withBroadcastSet(numTrackedHosts, "numTrackedHosts")
      .filter { _._2 >= 0.0001 }

    topTrackers.writeAsCsv(outputPath, fieldDelimiter = "\t", writeMode = WriteMode.OVERWRITE)

    env.execute()
  }

  class TrackerProbability() extends RichMapFunction[(String, Long), (String, Double)] {

    override def map(trackedIdWithCount: (String, Long)): (String, Double) = {

      val numTrackedHosts = getRuntimeContext.getBroadcastVariable[Tuple1[Long]]("numTrackedHosts").get(0)._1

      trackedIdWithCount._1 -> trackedIdWithCount._2.toDouble / numTrackedHosts
    }
  }

}