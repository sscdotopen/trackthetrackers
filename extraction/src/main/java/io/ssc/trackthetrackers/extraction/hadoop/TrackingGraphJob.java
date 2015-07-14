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
import io.ssc.trackthetrackers.extraction.hadoop.util.TopPrivateDomainExtractor;
import io.ssc.trackthetrackers.extraction.hadoop.writables.TrackingHostWithType;
import io.ssc.trackthetrackers.extraction.hadoop.writables.TrackingHostsWithTypes;
import io.ssc.trackthetrackers.extraction.resources.TrackingType;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import parquet.proto.ProtoParquetInputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

    Job thirdPartyGraph = mapReduce(inputPath, outputPath, ProtoParquetInputFormat.class, TextOutputFormat.class,
        EdgeListMapper.class, Text.class, TrackingHostsWithTypes.class, DistinctifyReducer.class,
        NullWritable.class, Text.class, false);

    thirdPartyGraph.getConfiguration().set("problemlog", new Path(outputPath, "problemlog").toString());

    thirdPartyGraph.waitForCompletion(true);

    return 0;
  }

  static class TrackingHostsCounter {
    private final Map<String, Map<TrackingType, Integer>> trackingHostsAndCounts = Maps.newHashMap();

    public void record(String trackingHost, TrackingType trackingType) {
      if (!trackingHostsAndCounts.containsKey(trackingHost)) {

        HashMap<TrackingType, Integer> typesAndCounts = Maps.newHashMap();
        typesAndCounts.put(TrackingType.SCRIPT, 0);
        typesAndCounts.put(TrackingType.IFRAME, 0);
        typesAndCounts.put(TrackingType.IMAGE, 0);
        typesAndCounts.put(TrackingType.LINK, 0);

        trackingHostsAndCounts.put(trackingHost, typesAndCounts);
      }

      Map<TrackingType, Integer> typesAndCounts = trackingHostsAndCounts.get(trackingHost);
      typesAndCounts.put(trackingType, typesAndCounts.get(trackingType) + 1);
    }

    public Set<Map.Entry<String, Map<TrackingType, Integer>>> counts() {
      return trackingHostsAndCounts.entrySet();
    }

  }

  static class DistinctifyReducer extends Reducer<Text, TrackingHostsWithTypes, NullWritable, Text> {

    private static final String SEPARATOR = "\t";

    @Override
    protected void reduce(Text trackedHost, Iterable<TrackingHostsWithTypes> trackingHostsWithTypes, Context ctx)
        throws IOException, InterruptedException {
      TrackingHostsCounter counter = new TrackingHostsCounter();

      int numPagesSeenFromDomain = 0;

      for (TrackingHostsWithTypes someTrackingHostsWithTypes : trackingHostsWithTypes) {
        for (TrackingHostWithType trackingHostWithType : someTrackingHostsWithTypes.values()) {
          String trackingHost = trackingHostWithType.trackingDomain();
          counter.record(trackingHost, trackingHostWithType.type());
        }
        numPagesSeenFromDomain++;
      }

      StringBuilder out = new StringBuilder("");

      out.append(trackedHost.toString());
      out.append(SEPARATOR);
      out.append(numPagesSeenFromDomain);
      out.append(SEPARATOR);



      for (Map.Entry<String, Map<TrackingType, Integer>> tracker : counter.counts()) {

        Map<TrackingType, Integer> counts = tracker.getValue();

        out.append("[");
        out.append(tracker.getKey());
        out.append(",");
        out.append(String.valueOf(counts.get(TrackingType.SCRIPT)));
        out.append(",");
        out.append(String.valueOf(counts.get(TrackingType.IFRAME)));
        out.append(",");
        out.append(String.valueOf(counts.get(TrackingType.IMAGE)));
        out.append(",");
        out.append(String.valueOf(counts.get(TrackingType.LINK)));
        out.append("]");
        out.append(SEPARATOR);
      }

      ctx.write(NullWritable.get(), new Text(out.toString().trim()));
    }
  }

  static class EdgeListMapper extends
      Mapper<Void, ParsedPageProtos.ParsedPage.Builder, Text, TrackingHostsWithTypes> {

    private BufferedWriter problemLogWriter;

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      super.setup(ctx);
      String randomID = UUID.randomUUID().toString();
      Path problemlog = new Path(ctx.getConfiguration().get("problemlog"), randomID);
      FileSystem fs = FileSystem.get(problemlog.toUri(), ctx.getConfiguration());

      problemLogWriter = new BufferedWriter(new OutputStreamWriter(fs.create(problemlog)));
    }

    private void writeToProblemLog(String marker, String message) throws IOException {
      problemLogWriter.write(marker);
      problemLogWriter.write("\t");
      problemLogWriter.write(message);
      problemLogWriter.newLine();
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
      super.cleanup(ctx);
      if (problemLogWriter != null) {
        problemLogWriter.close();
      }
    }

    public void map(Void key, ParsedPageProtos.ParsedPage.Builder parsedPageBuilder, Context ctx) throws IOException,
        InterruptedException {

      if (parsedPageBuilder != null) {
        ParsedPageProtos.ParsedPage parsedPage = parsedPageBuilder.build();
        if (parsedPage != null) {

          String trackedHost;

          try {
            trackedHost = TopPrivateDomainExtractor.extract(parsedPage.getUrl());
          } catch (Exception e) {
            ctx.getCounter(JobCounters.INVALID_TRACKED_HOST_DOMAINS).increment(1);
            writeToProblemLog("INVALID_TRACKED_HOST_DOMAINS", parsedPage.getUrl());
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
              String trackingHost = TopPrivateDomainExtractor.extract(trackingDomain.trackingDomain());
              if (!trackedHost.equalsIgnoreCase(trackingHost)) {
                trackingHostsWithType.add(new TrackingHostWithType(trackingHost, trackingDomain.type()));
              }
            } catch (Exception e) {
              ctx.getCounter(JobCounters.INVALID_TRACKING_HOST_DOMAINS).increment(1);
              writeToProblemLog("INVALID_TRACKING_HOST_DOMAINS", trackingDomain.trackingDomain());
            }
          }

          ctx.write(new Text(trackedHost), new TrackingHostsWithTypes(trackingHostsWithType));
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