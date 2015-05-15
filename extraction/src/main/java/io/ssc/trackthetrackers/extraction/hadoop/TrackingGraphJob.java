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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.ssc.trackthetrackers.commons.proto.ParsedPageProtos;
import io.ssc.trackthetrackers.extraction.hadoop.util.DomainIndex;
import io.ssc.trackthetrackers.extraction.hadoop.util.DistributedCacheHelper;
import io.ssc.trackthetrackers.extraction.hadoop.util.IndexNotFoundException;
import io.ssc.trackthetrackers.extraction.hadoop.util.TopPrivateDomainExtractor;
import io.ssc.trackthetrackers.extraction.hadoop.writables.TrackingHostWithType;
import io.ssc.trackthetrackers.extraction.hadoop.writables.TrackingHostsWithTypes;
import io.ssc.trackthetrackers.extraction.resources.TrackingType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import parquet.proto.ProtoParquetInputFormat;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;

public class TrackingGraphJob extends HadoopJob {

  public enum JobCounters {
    INVALID_TRACKED_HOST_DOMAINS, INVALID_TRACKING_HOST_DOMAINS;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TrackingGraphJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    Map<String, String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job toEdgeList = mapReduce(inputPath, outputPath, ProtoParquetInputFormat.class, TextOutputFormat.class,
        EdgeListMapper.class, IntWritable.class, TrackingHostsWithTypes.class, DistinctifyReducer.class,
        NullWritable.class, Text.class, false);

    Path domainIndex = new Path(parsedArgs.get("--domainIndex"));
    DistributedCacheHelper.cacheFile(domainIndex, toEdgeList.getConfiguration());

    toEdgeList.waitForCompletion(true);

    return 0;
  }

  static class DistinctifyReducer extends Reducer<IntWritable, TrackingHostsWithTypes, NullWritable, Text> {

    private static final String SEPARATOR = "\t";

    @Override
    protected void reduce(IntWritable trackedHost, Iterable<TrackingHostsWithTypes> trackingHostsWithTypes, Context ctx)
        throws IOException, InterruptedException {
      Map<String, Set<TrackingType>> trackingHosts = Maps.newHashMap();
      int trackedHostIndex = trackedHost.get();

      for (TrackingHostsWithTypes someTrackingHostsWithTypes : trackingHostsWithTypes) {
        for (TrackingHostWithType trackingHostWithType : someTrackingHostsWithTypes.values()) {

          //int trackingHostIndex = trackingHostWithType.trackingDomain();
          //if (trackingHostIndex != trackedHostIndex) {
          String trackingHost = trackingHostWithType.trackingDomain();
            if (!trackingHosts.containsKey(trackingHost)) {
              trackingHosts.put(trackingHost, Sets.<TrackingType>newHashSet());
            }
            trackingHosts.get(trackingHost).add(trackingHostWithType.type());
          //}
        }
      }

      for (Map.Entry<String, Set<TrackingType>> tracker : trackingHosts.entrySet()) {

        Set<TrackingType> types = tracker.getValue();

        String line = String.valueOf(trackedHostIndex) + SEPARATOR
                    + tracker.getKey() + SEPARATOR
                    + (types.contains(TrackingType.SCRIPT) ? "1" : "0") + SEPARATOR
                    + (types.contains(TrackingType.IFRAME) ? "1" : "0") + SEPARATOR
                    + (types.contains(TrackingType.IMAGE)  ? "1" : "0") + SEPARATOR
                    + (types.contains(TrackingType.LINK)   ? "1" : "0");

        ctx.write(NullWritable.get(), new Text(line));
      }
    }
  }

  static class EdgeListMapper extends
      Mapper<Void, ParsedPageProtos.ParsedPage.Builder, IntWritable, TrackingHostsWithTypes> {

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

    public void map(Void key, ParsedPageProtos.ParsedPage.Builder parsedPageBuilder, Context ctx) throws IOException,
        InterruptedException {

      if (parsedPageBuilder != null) {
        ParsedPageProtos.ParsedPage parsedPage = parsedPageBuilder.build();
        if (parsedPage != null) {

          String uri;
          int trackedHostIndex;
          try {
            uri = new URI(parsedPage.getUrl()).getHost().toString();
            String trackedHost = TopPrivateDomainExtractor.extract(uri);
            trackedHostIndex = domainIndex.indexFor(trackedHost);
          } catch (Exception e) {
            ctx.getCounter(JobCounters.INVALID_TRACKED_HOST_DOMAINS).increment(1);
            return;
          }

          Set<TrackingHostWithType> trackingDomainsWithTypes = Sets.newHashSet();
          addDomains(trackingDomainsWithTypes, parsedPage.getScriptsList(), TrackingType.SCRIPT);
          addDomains(trackingDomainsWithTypes, parsedPage.getIframesList(), TrackingType.IFRAME);
          addDomains(trackingDomainsWithTypes, parsedPage.getImagesList(), TrackingType.IMAGE);
          addDomains(trackingDomainsWithTypes, parsedPage.getLinksList(), TrackingType.LINK);

          Set<TrackingHostWithType> trackingHostsWithType = Sets.newHashSet();
          for (TrackingHostWithType trackingDomain : trackingDomainsWithTypes) {
            try {

              boolean isSameHost = false;
              try {
                int trackingHostIndex = domainIndex.indexFor(trackingDomain.trackingDomain());
                isSameHost = trackedHostIndex == trackingHostIndex;
              } catch (IndexNotFoundException e) {}

              if (!isSameHost) {
                String trackingHost = TopPrivateDomainExtractor.extract(trackingDomain.trackingDomain());
                trackingHostsWithType.add(new TrackingHostWithType(trackingHost, trackingDomain.type()));
              }
            } catch (Exception e) {
              ctx.getCounter(JobCounters.INVALID_TRACKING_HOST_DOMAINS).increment(1);
            }
          }

          ctx.write(new IntWritable(trackedHostIndex), new TrackingHostsWithTypes(trackingHostsWithType));
        }
      }
    }

    private void addDomains(Set<TrackingHostWithType> trackingDomainsWithTypes, Iterable<String> domains,
                            TrackingType type) {
      for (String domain : domains) {
        trackingDomainsWithTypes.add(new TrackingHostWithType(domain, type));
      }
    }
  }
}