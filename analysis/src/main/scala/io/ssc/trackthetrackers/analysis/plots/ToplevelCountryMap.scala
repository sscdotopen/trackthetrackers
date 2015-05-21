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

package io.ssc.trackthetrackers.analysis.plots

import java.io.{FileWriter, File}

import io.ssc.trackthetrackers.analysis.algorithms.plots.ChoroplethMapAppTracker
import org.apache.flink.shaded.com.google.common.io.Closeables
import processing.core.PApplet

import scala.collection.mutable
import scala.io.Source

import io.ssc.trackthetrackers.Config

import io.ssc.trackthetrackers.analysis.statistics._

object ToplevelCountryMap extends App {

  val company = "Google"

  var writer: FileWriter = null
  try {
    writer = new FileWriter(Config.get("company.distribution.by.country"), true)

    val lines = Source.fromFile(new File(Config.get("topleveldomainByCountry.csv"))).getLines

    for (domainCountry <- lines) {
      val splits = domainCountry.split(",")

      CompanyProbabilities.compute(Config.get("analysis.trackingraphsample.path"), Config.get("webdatacommons.pldfile.unzipped"),
        Config.get("analysis.results.path") + "companyDistribution", splits(0).toLowerCase, null, 0)

      for (file <- new File(Config.get("analysis.results.path") + "companyDistribution").listFiles) {
        val dataEntries = Source.fromFile(file).getLines
        for (dataEntry <- dataEntries) {
          val tokens = dataEntry.split("\t")

          if (tokens(0).equals(company)) {
            writer.write(splits(0) + "," + splits(1) + "," + splits(2) + "," + company + "," + tokens(1) + "\n")
          }
        }
      }
      writer.flush()
    }
  } finally {
    Closeables.close(writer, false)
  }
  
  val mapApp = new ChoroplethMapAppTracker()
  PApplet.main(Array("io.ssc.trackthetrackers.analysis.algorithms.plots.ChoroplethMapAppTracker"))
}
