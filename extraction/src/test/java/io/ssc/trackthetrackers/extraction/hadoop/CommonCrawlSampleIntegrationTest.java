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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class CommonCrawlSampleIntegrationTest {

  public static void main(String[] args) throws Exception {

    Path extractionOutput = new Path("/tmp/commoncrawl-extraction/");
    Path trackingGraphOutput = new Path("/tmp/commoncrawl-trackingraph/");

    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(extractionOutput, true);
    fs.delete(trackingGraphOutput, true);

    String inputs =
        Config.get("commoncrawl.samples.path") + "/1346914268347_246.arc.gz" + "," +
        Config.get("commoncrawl.samples.path") + "/1346916258648_2154.arc.gz" + "," +
        Config.get("commoncrawl.samples.path") + "/1346916752362_1803.arc.gz" + "," +
        Config.get("commoncrawl.problematic.path") + "/1346914042849_2441.arc.gz";

    ToolRunner.run(new ExtractionJob(), new String[] {
        "--input", inputs,
        "--output", extractionOutput.toString()
    });

    ToolRunner.run(new TrackingGraphJob(), new String[] {
        "--input", extractionOutput.toString(),
        "--output", trackingGraphOutput.toString()
    });
  }
}