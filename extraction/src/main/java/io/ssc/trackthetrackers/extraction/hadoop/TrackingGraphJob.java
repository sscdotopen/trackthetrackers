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
import io.ssc.trackthetrackers.extraction.hadoop.util.HadoopUtil;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import parquet.proto.ProtoParquetInputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;

public class TrackingGraphJob extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job toEdgeList = mapOnly(inputPath, outputPath, ProtoParquetInputFormat.class, TextOutputFormat.class,
                             EdgeListMapper.class, IntWritable.class, IntWritable.class, true);

    Path domainIndex = new Path(parsedArgs.get("--domainIndex"));

    DistributedCache.setCacheFiles(new URI[] { domainIndex.toUri() }, toEdgeList.getConfiguration());

    toEdgeList.waitForCompletion(true);

    return 0;
  }

  static class EdgeListMapper extends Mapper<Void, ParsedPageProtos.ParsedPage.Builder, IntWritable, IntWritable> {

    private static DomainIndex domainIndex;

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      Path domainIndexFile = HadoopUtil.getCachedFiles(ctx.getConfiguration())[0];
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

            for (String trackingDomain : allTrackingDomains) {
              String trackingHost = InternetDomainName.from(trackingDomain).topPrivateDomain().toString();
              if (!trackingHost.equals(trackedHost)) {
                int trackingHostIndex = domainIndex.indexFor(trackingHost);
                ctx.write(new IntWritable(trackingHostIndex), new IntWritable(trackedHostIndex));
              }
            }

          } catch (Exception e) {
            //TODO counter
          }
        }
      }
    }
  }
}
