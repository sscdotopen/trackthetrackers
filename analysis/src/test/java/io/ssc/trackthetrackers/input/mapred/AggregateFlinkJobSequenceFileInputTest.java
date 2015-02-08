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

package io.ssc.trackthetrackers.input.mapred;

import java.io.File;

public class AggregateFlinkJobSequenceFileInputTest {

  public static void main(String[] args) throws Exception {
    String inputPath = "/tmp/commoncrawl-extraction/mapred/";
    File f = new File(inputPath);
    
    if (!f.exists()) {
      throw new Exception("You have to run CheckMapreduceImplementationTest.java of package extraction first!");
    }
      
    AggregateFlinkJobSequenceFileInput.run("/tmp/commoncrawl-extraction/mapred/");
  }
}
