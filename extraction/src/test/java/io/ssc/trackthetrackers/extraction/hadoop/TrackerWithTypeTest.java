/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz, Karim Wadie
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

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.extraction.hadoop.TrackerWithType;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TrackerWithTypeTest {

  /*
   * Will read the sample corpus again and build the tracking graph while
   * comparing it to the tracking graph file under test.
   */
  @Test
  public void correctMask() throws Exception {
    TrackingGraphTestJob trackingGraphTestJob = new TrackingGraphTestJob();

    int result = ToolRunner.run(trackingGraphTestJob, new String[] { "--input", "/tmp/commoncrawl-extraction/", "--output",
        "/tmp/commoncrawl-test-trackinggraph/", "--domainIndex", Config.get("webdatacommons.pldfile"),
        "--trackingGraphUnderTest", Config.get("trackingtype.sample.withmask") });

    assertTrue("The hadoop testing job has failed. Examine the output/log for error details", result == 0);
  }

  /*
   * This utility method belongs to an out-dated test but can be used in the future to compare output files
   */
  private Set<String> buildSetFromFile(String filePath) throws IOException {
      BufferedReader bufferReader;
      Set<String> fileEntries = Sets.newHashSet();
      bufferReader = new BufferedReader(new FileReader(filePath));
      String line = bufferReader.readLine();
      while (line != null) {
        String[] tokins = line.split("\\s+");
        fileEntries.add(tokins[0] + "#" + tokins[1]);
        line = bufferReader.readLine();
      }
      bufferReader.close();
      return fileEntries;
  }
}