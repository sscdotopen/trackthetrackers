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


import com.google.common.io.Closeables;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;


public class DomainIndex {

  private final Long2IntMap hashesToIndices;

  public DomainIndex(FileSystem fs, Path indexFile) throws IOException {
    hashesToIndices = hashIndexFile(fs, indexFile);
  }

  public int indexFor(String paylevelDomain) {
    long hash = hash(paylevelDomain);
    int index = hashesToIndices.get(hash);

    if (index == 0) {
      throw new IllegalStateException("Unknown paylevelDomain: " + paylevelDomain);
    }

    return index;
  }

  private static long hash(String paylevelDomain) {
    return MurmurHash.hash64(paylevelDomain);
  }

  //TODO replace sysout's with log statements
  private Long2IntMap hashIndexFile(FileSystem fs, Path indexFile) throws IOException {

    // hardcoded to handle 2012 payleveldomain index
    Long2IntMap hashesToIndices = new Long2IntOpenHashMap(42889800);

    Pattern SEP = Pattern.compile("\t");
    int linesRead = 0;
    BufferedReader reader = null;

    try {
      reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(fs.open(indexFile)), "UTF8"));

      String line = null;
      while ((line = reader.readLine()) != null) {

        String[] tokens = SEP.split(line);

        long hash = hash(tokens[0]);
        int index = Integer.parseInt(tokens[1]);

        int previous = hashesToIndices.put(hash, index);

        if (previous != 0) {
          throw new IllegalStateException("Hash collision encountered!");
        }

        if (++linesRead % 100000 == 0) {
          System.out.println(linesRead + " lines read...");
        }
      }
    } finally {
      Closeables.close(reader, false);
    }

    System.out.println(linesRead + " lines read. done.");
    return hashesToIndices;
  }
}
