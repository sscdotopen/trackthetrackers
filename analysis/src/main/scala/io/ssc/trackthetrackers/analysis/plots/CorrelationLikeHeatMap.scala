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

import java.awt.Color

import org.jfree.chart.{ChartPanel, JFreeChart}
import org.jfree.chart.axis._
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.chart.renderer.LookupPaintScale
import org.jfree.chart.renderer.xy.XYBlockRenderer
import org.jfree.chart.title.PaintScaleLegend
import org.jfree.data.xy.DefaultXYZDataset
import org.jfree.ui.{RefineryUtilities, RectangleEdge, RectangleAnchor, ApplicationFrame}

class CorrelationLikeHeatMap(title: String, xLabel: String, yLabel: String, legendLabel: String,
                             entities: Array[String], entries: Array[Double]) extends ApplicationFrame(title) {

  val numEntries = entries.size

  val xvalues: Array[Double] = new Array[Double](numEntries)
  val yvalues: Array[Double] = new Array[Double](numEntries)

  for (i <- 0 until entries.size) {
    xvalues(i) = i / entities.size
    yvalues(i) = i % entities.size
  }

  val dataset: DefaultXYZDataset = new DefaultXYZDataset
  dataset.addSeries("Series 1", Array[Array[Double]](xvalues, yvalues, entries))

  val xAxis = new SymbolAxis(xLabel, entities)
  xAxis.setLowerBound(0.0)
  xAxis.setUpperBound(entities.size)
  xAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)

  val yAxis = new SymbolAxis(yLabel, entities)
  yAxis.setLowerBound(0.0)
  yAxis.setUpperBound(entities.size)
  yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)

  val renderer = new XYBlockRenderer
  renderer.setSeriesOutlinePaint(0, Color.blue)
  renderer.setBlockAnchor(RectangleAnchor.BOTTOM_LEFT)

  val paintScale = new LookupPaintScale(0, 1.0, Color.black)

  val offset = 100.0
  val intervals = 5
  for (n <- 0 to intervals) {
    paintScale.add(n * (1.0 / intervals), new Color((offset + (n * ((255.0 - offset) / intervals))).toInt, 0, 0))
  }

  renderer.setPaintScale(paintScale)

  val valueAxis1 = new NumberAxis("Value1")
  valueAxis1.setLabel(legendLabel)
  val psl: PaintScaleLegend = new PaintScaleLegend(paintScale, valueAxis1)
  psl.setPosition(RectangleEdge.RIGHT)
  psl.setAxisLocation(AxisLocation.BOTTOM_OR_RIGHT)
  psl.setMargin(0.0, 20.0, 40.0, 0.0)

  val plot = new XYPlot(dataset, xAxis, yAxis, renderer)
  plot.setOrientation(PlotOrientation.HORIZONTAL)
  plot.setBackgroundPaint(Color.RED)
  plot.setRangeGridlinePaint(Color.white)
  plot.setDomainGridlinePaint(Color.white)
  plot.setOrientation(PlotOrientation.HORIZONTAL)

  val chart = new JFreeChart(title, plot)
  chart.addSubtitle(psl)
  chart.removeLegend
  chart.setBackgroundPaint(Color.white)

  val chartPanel = new ChartPanel(chart)
  setContentPane(chartPanel)

  pack()
  RefineryUtilities.centerFrameOnScreen(this)
  setVisible(true)
}
