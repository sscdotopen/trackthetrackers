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

package io.ssc.trackthetrackers.extraction.hadoop;

import io.ssc.trackthetrackers.Config;
import org.apache.hadoop.util.ToolRunner;

public class CommonCrawlSampleIntegrationTest {

  public static void main(String[] args) throws Exception {

    ExtractionJob extraction = new ExtractionJob();
    TrackingGraphJob trackingGraph = new TrackingGraphJob();

    ToolRunner.run(extraction, new String[] {
        "--input", Config.get("commoncrawl.samples.path"),
        "--output", "/tmp/commoncrawl-extraction/"
    });

    ToolRunner.run(trackingGraph, new String[] {
        "--input", "/tmp/commoncrawl-extraction/",
        "--output", "/tmp/commoncrawl-trackingraph/",
        "--domainIndex", Config.get("webdatacommons.pldfile")
    });
  }
}
