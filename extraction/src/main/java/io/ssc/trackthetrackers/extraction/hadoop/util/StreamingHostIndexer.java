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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;

public class StreamingHostIndexer {

  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    index(fs, new Path("/tmp/commoncrawl-trackingraph/trackingHosts/"),
        new Path("/tmp/commoncrawl-trackingraph/indexedTrackingHosts.tsv"), conf);
  }

  public static void index(FileSystem fs, Path path, Path outPath, Configuration conf) throws IOException {

    BufferedWriter writer = null;
    try {
     int index = 0;
      writer = new BufferedWriter(new OutputStreamWriter(fs.create(outPath, true)));

      FileStatus[] statuses = fs.listStatus(path, new PartsFilter());
      Arrays.sort(statuses);

      for (FileStatus status : statuses) {
        SequenceFile.Reader reader = null;
        try {
          reader = new SequenceFile.Reader(fs, status.getPath(), conf);
          Text host = new Text();
          while (reader.next(host, NullWritable.get())) {
            writer.write(String.valueOf(index));
            writer.write('\t');
            writer.write(host.toString());
            writer.newLine();
            index++;
          }
        } finally {
          Closeables.close(reader, true);
        }
      }
    } finally {
      Closeables.close(writer, false);
    }
  }

  static class PartsFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return path.getName().contains("part-");
    }
  }

}
