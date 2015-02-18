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

package io.ssc.trackthetrackers.analysis.plots

import java.awt.{Dimension, Color}
import javax.swing.JFrame

import org.jfree.chart.{ChartPanel, ChartFactory}
import org.jfree.chart.axis.CategoryLabelPositions
import org.jfree.chart.plot.{CategoryPlot, PlotOrientation}
import org.jfree.chart.renderer.category.{StandardBarPainter, BarRenderer}
import org.jfree.data.category.CategoryDataset

class SingleSeriesBarChart(title: String, xLabel: String, yLabel: String, barColor: Color, dataset: CategoryDataset)
  extends JFrame {

  val chart = ChartFactory.createBarChart(
    title,
    xLabel,
    yLabel,
    dataset,
    PlotOrientation.VERTICAL,
    false,
    true,
    false
  )

  val plot = chart.getPlot().asInstanceOf[CategoryPlot]
  val xAxis = plot.getDomainAxis()

  xAxis.setCategoryLabelPositions(CategoryLabelPositions.DOWN_45)
  plot.setRangeGridlinesVisible(true)
  plot.setRangeGridlinePaint(Color.BLACK)
  plot.setBackgroundPaint(Color.WHITE)

  val renderer = plot.getRenderer.asInstanceOf[BarRenderer]
  renderer.setBarPainter(new StandardBarPainter())

  renderer.setSeriesPaint(0, barColor)

  val chartPanel = new ChartPanel(chart)
  chartPanel.setPreferredSize(new Dimension(1024, 768))
  getContentPane().add(chartPanel)

  pack()
  setVisible(true)
  setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)

}