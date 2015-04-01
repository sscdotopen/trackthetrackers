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

package io.ssc.trackthetrackers.analysis.algorithms.plots;

import de.fhpotsdam.unfolding.UnfoldingMap;
import de.fhpotsdam.unfolding.data.Feature;
import de.fhpotsdam.unfolding.data.GeoJSONReader;
import de.fhpotsdam.unfolding.marker.Marker;
import de.fhpotsdam.unfolding.utils.LargeMapImageUtils;
import de.fhpotsdam.unfolding.utils.MapUtils;
import io.ssc.trackthetrackers.Config;
import processing.core.PApplet;
import processing.core.PImage;

import java.util.HashMap;
import java.util.List;

/**
 * Visualizes distribution of a certain tracking company as a choropleth map. Countries are colored
 * in proportion to the distribution of a certain tracking company
 * <p/>
 * It loads the country shapes from a GeoJSON file via a data reader, and loads the company distribution values from
 * another CSV file. The data value is encoded to color via a simplistic linear mapping.
 */
public class ChoroplethMapAppTracker extends PApplet {

  UnfoldingMap map;

  LargeMapImageUtils lmiUtils;

  HashMap<String, DataEntry> dataEntriesMap;
  List<Marker> countryMarkers;

  public void setup() {
    size(800, 600, OPENGL);
    smooth();

    map = new UnfoldingMap(this, 50, 50, 700, 500);
    map.zoomToLevel(2);
    map.setBackgroundColor(240);
    MapUtils.createDefaultEventDispatcher(this, map);

    // Load country polygons and adds them as markers
    List<Feature> countries = GeoJSONReader.loadData(this, Config.get("countries.geo.json"));
    countryMarkers = MapUtils.createSimpleMarkers(countries);
    map.addMarkers(countryMarkers);

    // Load company distribution data
    dataEntriesMap = loadTrackerDensityFromCSV(Config.get("company.distribution.by.country"));
    println("Loaded " + dataEntriesMap.size() + " data entries");

    // Country markers are colored according to its company distribution (only once)
    shadeCountries();

    lmiUtils = new LargeMapImageUtils(this, map);
  }

  public void draw() {
    background(240);

    // Draw map tiles and country markers
    map.draw();

    lmiUtils.run();
  }

  public void keyPressed() {
    if (key == 's') {
      // Around current center and with current zoom level
      PImage snapshot = lmiUtils.makeSnapshot();
      snapshot.save("pic.png");
    }
  }

  public void shadeCountries() {
    for (Marker marker : countryMarkers) {
      // Find data for country of the current marker
      String countryId = marker.getId();
      DataEntry dataEntry = dataEntriesMap.get(countryId);

      float transparency = 200;

      if (dataEntry != null && dataEntry.value != null) {
        // Encode value as brightness (values range: 0-1000)
        /*
        float transparency = map(dataEntry.value, 0.0f, 1.0f, 10, 255);
        System.out.println("transparency: " + transparency);
        marker.setColor(color(255, 0, 0, transparency));*/

        float red = PApplet.map(dataEntry.value, 0.0f, 1.0f, 0, 255);
        float blue = PApplet.map((1.0f - dataEntry.value), 0.0f, 1.0f, 0, 255);
        marker.setColor(color(red, 0, blue, transparency));


        } else {
          // No value available
          marker.setColor(color(100, 120));
        }
      }
    }

    public HashMap<String, DataEntry> loadTrackerDensityFromCSV(String fileName) {
      HashMap<String, DataEntry> dataEntriesMap = new HashMap<String, DataEntry>();

      String[] rows = loadStrings(fileName);
      for (String row : rows) {
        // Reads country name and company distribution value from CSV row
        String[] columns = row.split(",");
        if (columns.length >= 5) {
          DataEntry dataEntry = new DataEntry();
          dataEntry.countryName = columns[1];
          dataEntry.id = columns[2];
          dataEntry.value = Float.parseFloat(columns[4]);
          dataEntriesMap.put(dataEntry.id, dataEntry);
        }
      }

      return dataEntriesMap;
    }

    class DataEntry {
      String countryName;
      String id;
      Float value;
    }
}
