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

package io.ssc.trackthetrackers.extraction.hadoop;

import io.ssc.trackthetrackers.extraction.proto.ParsedPageProtos;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import parquet.proto.ProtoParquetInputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AggregateScriptWatchersJob extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job job = mapReduce(inputPath, outputPath, ProtoParquetInputFormat.class, SequenceFileOutputFormat.class,
                        WatchersMapper.class, null, null, CountWatchingsReducer.class, Text.class, LongWritable.class,
                        true, true);

    job.waitForCompletion(true);

    return 0;
  }


  static class WatchersMapper extends Mapper<Void, ParsedPageProtos.ParsedPage.Builder, Text, LongWritable> {

    private final Text watcher = new Text();
    private final LongWritable one = new LongWritable(1);

    public void map(Void key, ParsedPageProtos.ParsedPage.Builder parsedPageBuilder,
        Mapper<Void,ParsedPageProtos.ParsedPage.Builder,Text,LongWritable>.Context context)
        throws IOException, InterruptedException {

      if (parsedPageBuilder != null) {
        ParsedPageProtos.ParsedPage parsedPage = parsedPageBuilder.build();
        if (parsedPage != null) {
          List<String> list = parsedPage.getScriptsList(); //TODO: why only scripts?
          if (list != null) {
            for (String aWatcher : list) {
              watcher.set(aWatcher);
              context.write(watcher, one);
            }
          }
        }
      }
    }
  }

  static class CountWatchingsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable count = new LongWritable();

    public void reduce(Text watcher, Iterable<LongWritable> counts, Context context)
        throws IOException, InterruptedException{

      long sum = 0;
      while (counts.iterator().hasNext()) {
        sum += counts.iterator().next().get();
      }
      count.set(sum);
      context.write(watcher, count);
    }
  }


}
