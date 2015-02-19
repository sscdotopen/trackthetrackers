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

import com.google.common.collect.Sets;
import com.google.common.net.InternetDomainName;
import io.ssc.trackthetrackers.commons.proto.ParsedPageProtos;
import io.ssc.trackthetrackers.extraction.hadoop.util.DomainIndex;
import io.ssc.trackthetrackers.extraction.hadoop.util.DistributedCacheHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import parquet.proto.ProtoParquetInputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TrackingGraphJob extends HadoopJob {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TrackingGraphJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job toEdgeList = mapReduce(inputPath, outputPath, ProtoParquetInputFormat.class, TextOutputFormat.class,
        EdgeListMapper.class, IntWritable.class, IntArrayWritable.class,
        DistinctifyReducer.class, IntWritable.class, IntWritable.class, false, true);

    Path domainIndex = new Path(parsedArgs.get("--domainIndex"));
    DistributedCacheHelper.cacheFile(domainIndex, toEdgeList.getConfiguration());

    toEdgeList.waitForCompletion(true);

    return 0;
  }

  static class DistinctifyReducer extends Reducer<IntWritable, IntArrayWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(IntWritable trackedHost, Iterable<IntArrayWritable> values, Context ctx)
        throws IOException, InterruptedException {

      Set<Integer> trackingHosts = new HashSet<Integer>();
      for (IntArrayWritable indices : values) {
        for (int index : indices.values()) {
          trackingHosts.add(index);
        }
      }

      int trackedHostIndex = trackedHost.get();
      trackingHosts.remove(trackedHostIndex);

      for (int trackingHostIndex : trackingHosts) {
        ctx.write(new IntWritable(trackingHostIndex), new IntWritable(trackedHostIndex));
      }
    }
  }


  static class EdgeListMapper extends Mapper<Void, ParsedPageProtos.ParsedPage.Builder, IntWritable, IntArrayWritable> {

    private static DomainIndex domainIndex;

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      Path domainIndexFile = DistributedCacheHelper.getCachedFiles(ctx.getConfiguration())[0];
      FileSystem fs = FileSystem.get(domainIndexFile.toUri(), ctx.getConfiguration());
      // potentially exploit VM re-use
      if (domainIndex == null) {
        domainIndex = new DomainIndex(fs, domainIndexFile);
      }
    }

    public void map(Void key, ParsedPageProtos.ParsedPage.Builder parsedPageBuilder, Context ctx)
        throws IOException, InterruptedException {

      if (parsedPageBuilder != null) {
        ParsedPageProtos.ParsedPage parsedPage = parsedPageBuilder.build();
        if (parsedPage != null) {
          try {

            String uri = new URI(parsedPage.getUrl()).getHost().toString();
            String trackedHost = InternetDomainName.from(uri).topPrivateDomain().toString();

            int trackedHostIndex = domainIndex.indexFor(trackedHost);

            Set<String> allTrackingDomains = Sets.newHashSet();
            allTrackingDomains.addAll(parsedPage.getScriptsList());
            allTrackingDomains.addAll(parsedPage.getIframesList());
            allTrackingDomains.addAll(parsedPage.getImagesList());
            allTrackingDomains.addAll(parsedPage.getLinksList());

            int[] trackingHosts = new int[allTrackingDomains.size()];

            int n = 0;
            for (String trackingDomain : allTrackingDomains) {
              String trackingHost = InternetDomainName.from(trackingDomain).topPrivateDomain().toString();
              int trackingHostIndex = domainIndex.indexFor(trackingHost);
              trackingHosts[n++] = trackingHostIndex;
            }

            ctx.write(new IntWritable(trackedHostIndex), new IntArrayWritable(trackingHosts));

          } catch (Exception e) {
            //TODO counter
          }
        }
      }
    }
  }
}
