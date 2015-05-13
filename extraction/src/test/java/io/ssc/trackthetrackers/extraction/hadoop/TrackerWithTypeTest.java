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
import java.io.FileReader;
import java.util.Set;
import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.extraction.hadoop.TrackerWithType;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import com.google.common.collect.Sets;

public class TrackerWithTypeTest {

  @Test
  public void uniqueSets() {
    TrackerWithType t1 = new TrackerWithType("a", 100, TrackingType.SCRIPT);
    TrackerWithType t2 = new TrackerWithType("a", 200, TrackingType.SCRIPT);
    TrackerWithType t3 = new TrackerWithType("a", 100, TrackingType.SCRIPT);
    TrackerWithType t4 = new TrackerWithType("a", 100, TrackingType.IFRAME);

    // t1 and t3 should be equal and not added twice
    Set<TrackerWithType> s = Sets.newHashSet();
    s.add(t1);
    s.add(t2);
    s.add(t3);
    s.add(t4);
    assertTrue(s.size() == 3);
  }

  /*
   * will test if the tracked graph on the commoncrawl sample is the same 
   * before and after adding the trackingtype mask
   */
  @Test
  public void sameOutput() {
    // read the original file into a hashmap
    String file1Path = Config.get("trackingtype.sample.withmask");
    String file2Path = Config.get("trackingtype.sample.nomask");
    Set<String> s1 = buildSetFromFile(file1Path);
    Set<String> s2 = buildSetFromFile(file2Path);

    assertTrue("The two sets are not equal in size. s1 =" + s1.size() + " s2=" + s2.size(), s1.size() == s2.size());
    assertTrue("The two sets are equal in size but with different entries", s1.equals(s2));
  }

  /*
   * Will read the sample corpus again and build the tracking graph while comparing
   * it to the tracking graph file under test.
   */
  @Test
  public void correctMask() throws Exception {
    TrackingGraphTestJob trackingGraphJob = new TrackingGraphTestJob();

    int result = ToolRunner.run(trackingGraphJob, new String[] { "--input", "/tmp/commoncrawl-extraction/", "--output",
        "/tmp/commoncrawl-test-trackinggraph/", "--domainIndex", Config.get("webdatacommons.pldfile"),
        "--trackingGraphUnderTest", Config.get("trackingtype.sample.withmask") });

    assertTrue("The hadoop testing job has failed. Examine the output/log for error details", result == 0);
  }

  private Set<String> buildSetFromFile(String filePath) {
    try {
      BufferedReader br;
      Set<String> map = Sets.newHashSet();
      br = new BufferedReader(new FileReader(filePath));
      String line = br.readLine();

      while (line != null) {

        String[] toks = line.split("\\s+");
        map.add(toks[0] + "#" + toks[1]);
        line = br.readLine();
      }

      br.close();
      return map;
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }
}
