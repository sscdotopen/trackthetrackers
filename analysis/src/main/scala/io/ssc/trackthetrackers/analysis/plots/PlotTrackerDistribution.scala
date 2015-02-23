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

import java.io.File

import org.jfree.data.category.DefaultCategoryDataset

import scala.io.Source

import java.awt._

import io.ssc.trackthetrackers.Config

object PlotTrackerDistribution extends App {

  var trackersWithProbability = Seq[(String, Double)]()

  for (file <- new File(Config.get("trackerDistribution.path")).listFiles) {
    trackersWithProbability ++= Source.fromFile(file).getLines.map { line =>
      val tokens = line.split("\t")
      tokens(0) -> tokens(1).toDouble
    }
  }

  val sortedTrackersWithProbability = trackersWithProbability.sortBy(_._2).reverse

  val dataset = new DefaultCategoryDataset()

  for ((tracker, probability) <- sortedTrackersWithProbability) {
    dataset.addValue(probability, "", tracker)
  }

  new SingleSeriesBarChart("tracking domain distribution", "tracking domain", "probability", Color.RED, dataset)
}

