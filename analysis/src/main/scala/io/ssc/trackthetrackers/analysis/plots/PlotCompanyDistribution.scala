package io.ssc.trackthetrackers.analysis.plots


import java.io.File

import org.jfree.data.category.DefaultCategoryDataset

import scala.io.Source

import java.awt._

object PlotCompanyDistribution extends App {

  var companiesWithProbability = Seq[(String, Double)]()

  for (file <- new File("/home/ssc/Desktop/trackthetrackers/out/companyDistribution/companyDistribution").listFiles) {
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

  new SingleSeriesBarChart("tracking company distribution", "company", "probability", Color.RED, dataset)
}

