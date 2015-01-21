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

package io.ssc.trackthetrackers.extraction.flink;

import io.ssc.trackthetrackers.extraction.flink.sequencefileinput.AggregateFlinkJobMapred;
import io.ssc.trackthetrackers.extraction.hadoop.mapred.ExtractionJobMapred;
import org.apache.hadoop.util.ToolRunner;
import io.ssc.trackthetrackers.extraction.hadoop.Config;

public class FlinkTest {

  public static void main(String[] args) throws Exception {
    
    ExtractionJobMapred extractionMapred = new ExtractionJobMapred();

    ToolRunner.run(extractionMapred, new String[] {
        "--input", Config.get("commoncrawl.samples.path"),
        "--output", "/tmp/commoncrawl-extraction/mapred/"
    });

    AggregateFlinkJobMapred.run("/tmp/commoncrawl-extraction/mapred/");
  }
}
