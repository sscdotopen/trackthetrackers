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

package io.ssc.trackthetrackers.analysis.datasets.cfindergoogle

import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}

import scala.io.Source
import scala.util.Random

/*
* http://konect.uni-koblenz.de/networks/cfinder-google
**/
object ToWebdataCommonsHyperlink extends App {

  val datasetDirectory = "/home/ssc/Desktop/tmp3/cfinder-google/"
  val urlsFile = datasetDirectory + "ent.cfinder-google.url.name"
  val linksFile = datasetDirectory + "out.cfinder-google"
  val outputDir = "/home/ssc/Entwicklung/projects/trackthetrackers/src/main/resources/cfindergoogle"

  var index = 0
  // evil map with side effects...
  val indexedUris = (Source.fromFile(urlsFile).getLines
    .map { line =>
      index += 1
      line + "\t" + index
    }).mkString("\n")

  val links = (Source.fromFile(linksFile).getLines
      .filter { line => !line.startsWith("%") }
      .map { _.trim.replaceAll(" ", "\t") }).mkString("\n")

  Files.write(Paths.get(outputDir + "/uris.tsv"), indexedUris.getBytes(StandardCharsets.UTF_8))
  Files.write(Paths.get(outputDir + "/links.tsv"), links.getBytes(StandardCharsets.UTF_8))
}

