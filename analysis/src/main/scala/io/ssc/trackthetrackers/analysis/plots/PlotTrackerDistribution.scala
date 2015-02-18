package io.ssc.trackthetrackers.analysis.plots


import java.io.File

import org.jfree.data.category.DefaultCategoryDataset

import scala.io.Source

import java.awt._

object PlotTrackerDistribution extends App {

  var trackersWithProbability = Seq[(String, Double)]()

  for (file <- new File("/home/ssc/Desktop/trackthetrackers/out/trackerDistribution/trackerDistribution").listFiles) {
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

