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

import java.awt._
import java.io.File

import io.ssc.trackthetrackers.Config
import io.ssc.trackthetrackers.analysis.statistics.CompanyProbabilities
import org.jfree.data.category.DefaultCategoryDataset

import scala.io.Source

object PlotCompanyDistributionByToplevelDomain extends App {

  val folder = Config.get("output.images")

  val lines = Source.fromFile(new File(Config.get("topleveldomainByCountry.csv"))).getLines

  for (domainCountry <- lines) {

    val splits = domainCountry.split(" = ")
    
    val toplevelDomain = splits(0).toLowerCase()

    CompanyProbabilities.compute(Config.get("analysis.trackingraphsample.path"), Config.get("webdatacommons.pldfile.unzipped"),
      Config.get("analysis.results.path") + "companyDistribution", toplevelDomain, null, 0)
    

    var companiesWithProbability = Seq[(String, Double)]()

    for (file <- new File(Config.get("analysis.results.path") + "companyDistribution").listFiles) {
      companiesWithProbability ++= Source.fromFile(file).getLines.map { line =>
        val tokens = line.split("\t")
        tokens(0) -> tokens(1).toDouble
      }
    }

    val sortedTrackersWithProbability = companiesWithProbability.sortBy(_._2).reverse

    val dataset = new DefaultCategoryDataset()

    for ((tracker, probability) <- sortedTrackersWithProbability) {
      dataset.addValue(probability, "", tracker)
    }
    
    val filename = folder + "chart-CompanyDistribution-ByToplevelDomain-" + toplevelDomain + ".png"

    new SingleSeriesBarChart("tracking company distribution", "company", "probability", Color.RED, dataset, filename)
  }
}

