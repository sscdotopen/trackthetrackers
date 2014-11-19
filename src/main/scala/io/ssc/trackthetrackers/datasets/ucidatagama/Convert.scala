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

package io.ssc.trackthetrackers.datasets.ucidatagama

import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}

import scala.io.Source


object Convert extends App {

  val linksFile = "/home/ssc/Desktop/tmp4/ucidata-gama/out.ucidata-gama"
  val outputDir = "/home/ssc/Entwicklung/projects/trackthetrackers/src/main/resources/ucidatagama"

  val indexedUris = (for (vertex <- 1 to 16) yield { vertex + "\t" + vertex }).mkString("\n")

  val links = (Source.fromFile(linksFile).getLines
    .filter { line => !line.startsWith("%") }
    .map { line =>
      val tokens = line.split("\t")
      tokens(0) + "\t" + tokens(1)
    }).mkString("\n")

  Files.write(Paths.get(outputDir + "/uris.tsv"), indexedUris.getBytes(StandardCharsets.UTF_8))
  Files.write(Paths.get(outputDir + "/links.tsv"), links.getBytes(StandardCharsets.UTF_8))

}
