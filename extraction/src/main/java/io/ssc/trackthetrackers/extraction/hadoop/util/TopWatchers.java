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

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@Deprecated
public class TopWatchers {

  private TopWatchers() {}

  public static void print(String path, int howMany) throws IOException {

    PriorityQueue<WatcherWithCount> queue = new PriorityQueue<WatcherWithCount>(howMany) {
      @Override
      protected boolean lessThan(WatcherWithCount a, WatcherWithCount b) {
        return a.count < b.count;
      }
    };

    SequenceFile.Reader reader = null;
    try {
      Configuration conf = new Configuration();
      reader = new SequenceFile.Reader(FileSystem.getLocal(conf), new Path(path), conf);
      Text watcher = new Text();
      LongWritable count = new LongWritable();
      while (reader.next(watcher, count)) {
        queue.insertWithOverflow(new WatcherWithCount(watcher.toString(), count.get()));
      }
    } finally {
      Closeables.close(reader, true);
    }

    List<WatcherWithCount> topWatchers = Lists.newArrayListWithCapacity(howMany);
    while (queue.size() > 0) {
      topWatchers.add(queue.pop());
    }
    Collections.reverse(topWatchers);

    for (WatcherWithCount watcherWithCount : topWatchers) {
      System.out.println(watcherWithCount);
    }
  }

  static class WatcherWithCount {

    private final String watcher;
    private final long count;

    WatcherWithCount(String watcher, long count) {
      this.watcher = watcher;
      this.count = count;
    }

    @Override
    public String toString() {
      return watcher + " " + count;
    }
  }

}
