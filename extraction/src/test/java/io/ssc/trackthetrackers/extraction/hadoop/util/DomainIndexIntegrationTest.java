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

package io.ssc.trackthetrackers.extraction.hadoop.util;


import io.ssc.trackthetrackers.extraction.hadoop.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class DomainIndexIntegrationTest {

  public static void main(String[] args) throws IOException {

    FileSystem fs = FileSystem.getLocal(new Configuration());

    Path indexFile = new Path(Config.get("webdatacommons.pldfile"));

    DomainIndex domainIndex = new DomainIndex(fs, indexFile);

    System.out.println("google-analytics.com " + domainIndex.indexFor("google-analytics.com"));
    System.out.println("spiegel.de " + domainIndex.indexFor("spiegel.de"));
    System.out.println("ssc.io " + domainIndex.indexFor("ssc.io"));
  }
}
