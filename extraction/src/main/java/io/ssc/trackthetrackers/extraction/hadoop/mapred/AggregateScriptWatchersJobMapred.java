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

package io.ssc.trackthetrackers.extraction.hadoop.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

public class AggregateScriptWatchersJobMapred extends HadoopJobMapred {

  public static enum JobCounters {
    PAGES, RESOURCES
  }

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    mapReduce(inputPath, outputPath, SequenceFileInputFormat.class, SequenceFileOutputFormat.class,
        WatchersMapper.class, Text.class, LongWritable.class, 
        CountWatchingsReducer.class, Text.class, LongWritable.class, true, true);
    
    return 0;
  }

  static class WatchersMapper extends MapReduceBase implements Mapper<Text, Text, Text, LongWritable> {

    private final Text watcher = new Text();
    private final LongWritable one = new LongWritable(1);

    private static final Pattern SEP = Pattern.compile(",");

    @Override
    public void map(Text url, Text watchers, OutputCollector<Text, LongWritable> collector, Reporter reporter)
        throws IOException {

      String[] allWatchers = SEP.split(watchers.toString());
      for (String aWatcher : allWatchers) {
        watcher.set(aWatcher);
        collector.collect(watcher, one);
      }

      reporter.incrCounter(JobCounters.PAGES, 1);
      reporter.incrCounter(JobCounters.RESOURCES, allWatchers.length);
    }
  }

  static class CountWatchingsReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable count = new LongWritable();

    @Override
    public void reduce(Text watcher, Iterator<LongWritable> counts, OutputCollector<Text, LongWritable> collector,
                       Reporter reporter) throws IOException {
      long sum = 0;
      while (counts.hasNext()) {
        sum += counts.next().get();
      }

      count.set(sum);
      collector.collect(watcher, count);
    }
  }
}
