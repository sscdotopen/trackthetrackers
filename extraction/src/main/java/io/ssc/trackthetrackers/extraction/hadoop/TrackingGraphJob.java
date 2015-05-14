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

import com.google.common.collect.Sets;
import com.google.common.net.InternetDomainName;
import io.ssc.trackthetrackers.commons.proto.ParsedPageProtos;
import io.ssc.trackthetrackers.extraction.hadoop.util.DomainIndex;
import io.ssc.trackthetrackers.extraction.hadoop.util.DistributedCacheHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import parquet.proto.ProtoParquetInputFormat;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TrackingGraphJob extends HadoopJob {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TrackingGraphJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    Map<String, String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job toEdgeList = mapReduce(inputPath, outputPath, ProtoParquetInputFormat.class, TextOutputFormat.class,
        EdgeListMapper.class, IntWritable.class, TwoDIntArrayWritable.class, DistinctifyReducer.class,
        IntWritable.class, Text.class, false);

    Path domainIndex = new Path(parsedArgs.get("--domainIndex"));
    DistributedCacheHelper.cacheFile(domainIndex, toEdgeList.getConfiguration());

    toEdgeList.waitForCompletion(true);

    return 0;
  }

  static class DistinctifyReducer extends Reducer<IntWritable, TwoDIntArrayWritable, IntWritable, Text> {

    private final String DELIM = "#";

    @Override
    protected void reduce(IntWritable trackedHost, Iterable<TwoDIntArrayWritable> values, Context ctx)
        throws IOException, InterruptedException {
      Map<Integer, String> trackingHosts = new HashMap<Integer, String>();
      int trackedHostIndex = trackedHost.get();

      // fill out the trackers and tracking type into a set
      for (TwoDArrayWritable trackersWithTypeMsg : values) {
        try {
          Writable[][] trackersWithType = trackersWithTypeMsg.get();
          for (int i = 0; i < trackersWithType.length; i++) {
            int trackingHostIndex = ((IntWritable) trackersWithType[i][0]).get();
            int trackingHostType = ((IntWritable) trackersWithType[i][1]).get();

            // don't add a link from the tracked node to itself concatenate the tracking types for each unique tracker-trackedsite
            if (trackingHostIndex != trackedHostIndex) {
              if (trackingHosts.containsKey(trackingHostIndex)) {
                trackingHosts.put(trackingHostIndex,
                    trackingHosts.get(trackingHostIndex) + String.valueOf(trackingHostType) + DELIM);
              } else {
                trackingHosts.put(trackingHostIndex, String.valueOf(trackingHostType) + DELIM);
              }
            }
          }
        } catch (Exception ex) {
          // TODO log or count
          continue;
        }
      }

      for (Integer trackingHost : trackingHosts.keySet()) {
        try {
          List<String> tags = Arrays.asList(trackingHosts.get(trackingHost).split(DELIM));
          String trackingTypeMask = "";

          // the TrackingType enum is serialized as it's ordinal int value, so parse it as int
          trackingTypeMask += tags.contains(String.valueOf(TrackingType.SCRIPT.ordinal())) ? "1" : "0";
          trackingTypeMask += tags.contains(String.valueOf(TrackingType.IFRAME.ordinal())) ? "1" : "0";
          trackingTypeMask += tags.contains(String.valueOf(TrackingType.IMAGE.ordinal())) ? "1" : "0";
          trackingTypeMask += tags.contains(String.valueOf(TrackingType.LINK.ordinal())) ? "1" : "0";

          String out = trackedHostIndex + "\t" + trackingTypeMask;
          ctx.write(new IntWritable(trackingHost), new Text(out));
        } catch (Exception ex) {
          // TODO log or count
          continue;
        }
      }
    }
  }

  static class EdgeListMapper extends
      Mapper<Void, ParsedPageProtos.ParsedPage.Builder, IntWritable, TwoDIntArrayWritable> {

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
      try {
        if (parsedPageBuilder != null) {
          ParsedPageProtos.ParsedPage parsedPage = parsedPageBuilder.build();
          if (parsedPage != null) {
            // try to get the topPrivateDomain of the tracked host. If the lookup failed ignore this parsed page
            String uri = "";
            int trackedHostIndex = 0;
            try {
              uri = new URI(parsedPage.getUrl()).getHost().toString();
              String trackedHost = InternetDomainName.from(uri).topPrivateDomain().toString();
              trackedHostIndex = domainIndex.indexFor(trackedHost);
            } catch (Exception e) {
              // TODO: Log or count
              return;
            }

            // extract all the potential trackers from the page
            Set<TrackerWithType> allTrackingDomains = Sets.newHashSet();
            allTrackingDomains.addAll(createTrackersWithType(parsedPage.getScriptsList(), TrackingType.SCRIPT));
            allTrackingDomains.addAll(createTrackersWithType(parsedPage.getIframesList(), TrackingType.IFRAME));
            allTrackingDomains.addAll(createTrackersWithType(parsedPage.getImagesList(), TrackingType.IMAGE));
            allTrackingDomains.addAll(createTrackersWithType(parsedPage.getLinksList(), TrackingType.LINK));

            // filter out the valid tracking hosts based on their topPrivateDomain and index lookup
            Set<TrackerWithType> trackingHosts = Sets.newHashSet();
            for (TrackerWithType trackingDomain : allTrackingDomains) {
              try {
                String trackingHost = InternetDomainName.from(trackingDomain.getTrackerDomain()).topPrivateDomain()
                    .toString();
                int trackingHostIndex = domainIndex.indexFor(trackingHost);
                TrackerWithType trackerWithIndex = new TrackerWithType("", trackingHostIndex,
                    trackingDomain.getTrackerType());
                trackingHosts.add(trackerWithIndex);
              } catch (Exception ex) {
                // TODO: Log or count
                // Do not stop if one tracker has failed in lookups
                continue;
              }
            }

            // copy the set containing valid trackers into 2DArray. Prepare for serialization
            IntWritable[][] trackersWithType = new IntWritable[trackingHosts.size()][2];
            int n = 0;
            for (TrackerWithType tracker : trackingHosts) {
              trackersWithType[n][0] = new IntWritable(tracker.getTrackerID());
              trackersWithType[n][1] = new IntWritable(tracker.getTrackerType().ordinal());
              n++;
            }

            // set the output to be serialized for reducers
            TwoDIntArrayWritable trackersWithTypeMsg = new TwoDIntArrayWritable();
            trackersWithTypeMsg.set(trackersWithType);

            ctx.write(new IntWritable(trackedHostIndex), trackersWithTypeMsg);
          }
        }
      } catch (Exception ex) {
        // TODO: Counter
        // in case an un-handled exception. do not fail the job
        return;
      }
    }

    private Collection<TrackerWithType> createTrackersWithType(List<String> trackers, TrackingType type) {
      List<TrackerWithType> trackerWithTypeList = new ArrayList<TrackerWithType>(trackers.size());
      for (String tracker : trackers){
        trackerWithTypeList.add(new TrackerWithType(tracker, 0, type));
      }
      return trackerWithTypeList;
    }
  }
}