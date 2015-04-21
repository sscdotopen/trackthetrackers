/**
 * Track the trackers
 * Copyright (C) 2014  Sebastian Schelter, Felix Neutatz
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import parquet.proto.ProtoParquetInputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TrackingGraphTestJob extends HadoopJob {

  @Override
  public int run(String[] args) {
    try {
      Map<String, String> parsedArgs = parseArgs(args);

      Path inputPath = new Path(parsedArgs.get("--input"));
      Path outPath = new Path(parsedArgs.get("--output"));

      Job toEdgeList = mapReduce(inputPath, outPath, ProtoParquetInputFormat.class, TextOutputFormat.class,
          EdgeListMapper.class, IntWritable.class, TwoDIntArrayWritable.class, DummyReducer.class, IntWritable.class,
          Text.class, false, true);

      Path domainIndex = new Path(parsedArgs.get("--domainIndex"));
      Path trackingGraphUnderTest = new Path(parsedArgs.get("--trackingGraphUnderTest"));

      DistributedCacheHelper.cacheFile(domainIndex, toEdgeList.getConfiguration());
      DistributedCacheHelper.cacheFile(trackingGraphUnderTest, toEdgeList.getConfiguration());

      toEdgeList.waitForCompletion(true);

      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
  }

  static class DummyReducer extends Reducer<IntWritable, TwoDIntArrayWritable, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable trackedHost, Iterable<TwoDIntArrayWritable> values, Context ctx)
        throws IOException, InterruptedException {
      // Do nothing
    }
  }

  static class EdgeListMapper extends
      Mapper<Void, ParsedPageProtos.ParsedPage.Builder, IntWritable, TwoDIntArrayWritable> {

    private static DomainIndex         domainIndex;

    // map of (tracker#tracked, trackingTypeMask)
    private static Map<String, String> trackingGraphUnderTest;

    public Map<String, String> trackingGraphFromFile(FileSystem localFile, Path path) throws Exception {
      BufferedReader br;
      Map<String, String> graphMap = new HashMap<String, String>();
      br = new BufferedReader(new InputStreamReader(localFile.open(path)));
      String line = br.readLine();

      while (line != null) {
        String[] toks = line.split("\\s+");
        if (toks.length < 3) {
          throw new Exception("Input file is missing a column. Format should be [tracker tracked trackingtypemask] ");
        }
        graphMap.put(toks[0] + "#" + toks[1], toks[2]);
        line = br.readLine();
      }
      br.close();
      return graphMap;
    }

    @Override
    protected void setup(Context ctx) throws IOException {
      Path domainIndexFile = DistributedCacheHelper.getCachedFiles(ctx.getConfiguration())[0];
      FileSystem fs = FileSystem.get(domainIndexFile.toUri(), ctx.getConfiguration());
      // potentially exploit VM re-use
      if (domainIndex == null) {
        domainIndex = new DomainIndex(fs, domainIndexFile);
      }

      Path trackingFileUnderTest = DistributedCacheHelper.getCachedFiles(ctx.getConfiguration())[1];
      FileSystem trackingFileFS = FileSystem.get(trackingFileUnderTest.toUri(), ctx.getConfiguration());
      if (trackingGraphUnderTest == null) {
        try {
          trackingGraphUnderTest = trackingGraphFromFile(trackingFileFS, trackingFileUnderTest);
        } catch (Exception e) {
          throw new IOException("building the graph map failed");
        }
      }

      if (trackingGraphUnderTest.size() == 0) {
        throw new IOException("graph map is empty");
      }
    }

    public void map(Void key, ParsedPageProtos.ParsedPage.Builder parsedPageBuilder, Context ctx) throws IOException,
        InterruptedException {

      ParsedPageProtos.ParsedPage parsedPage = parsedPageBuilder.build();
      if (parsedPage != null) {

        String uri = "";
        int trackedHostIndex = 0;
        try {
          uri = new URI(parsedPage.getUrl()).getHost().toString();
          String trackedHost = InternetDomainName.from(uri).topPrivateDomain().toString();
          trackedHostIndex = domainIndex.indexFor(trackedHost);
        } catch (Exception e) {
          return;
        }

        Set<TrackerWithType> allTrackingDomains = Sets.newHashSet();

        allTrackingDomains.addAll(createTrackersWithType(parsedPage.getScriptsList(), TrackingType.SCRIPT));
        allTrackingDomains.addAll(createTrackersWithType(parsedPage.getIframesList(), TrackingType.IFRAME));
        allTrackingDomains.addAll(createTrackersWithType(parsedPage.getImagesList(), TrackingType.IMAGE));
        allTrackingDomains.addAll(createTrackersWithType(parsedPage.getLinksList(), TrackingType.LINK));

        for (TrackerWithType trackingDomain : allTrackingDomains) {
          try {
            String trackingHost = InternetDomainName.from(trackingDomain.getTrackerDomain()).topPrivateDomain()
                .toString();
            int trackingHostIndex = domainIndex.indexFor(trackingHost);

            // ignore this case since it is omitted from the graph in the first
            // place
            if (trackingHostIndex == trackedHostIndex) {
              continue;
            }

            String mapKey = trackingHostIndex + "#" + trackedHostIndex;
            String trackingMask = trackingGraphUnderTest.get(mapKey);

            if (trackingMask == null || trackingMask.equals("")) {
              throw new InterruptedException("entry not found in the map. This shouldn't happen. key is " + mapKey
                  + " and map size is " + trackingGraphUnderTest.size());
            }
            if (trackingMask.charAt(0) == 1 && trackingDomain.getTrackerType() != TrackingType.SCRIPT) {
              throw new InterruptedException("mask shows that tracker " + trackingHostIndex
                  + " is found as SCRIPT in site " + trackedHostIndex + " but it is not");
            }
            if (trackingMask.charAt(1) == 1 && trackingDomain.getTrackerType() != TrackingType.IFRAME) {
              throw new InterruptedException("mask shows that tracker " + trackingHostIndex
                  + " is found as IFRAME in site " + trackedHostIndex + " but it is not");
            }
            if (trackingMask.charAt(2) == 1 && trackingDomain.getTrackerType() != TrackingType.IMAGE) {
              throw new InterruptedException("mask shows that tracker " + trackingHostIndex
                  + " is found as IMAGE in site " + trackedHostIndex + " but it is not");
            }
            if (trackingMask.charAt(3) == 1 && trackingDomain.getTrackerType() != TrackingType.LINK) {
              throw new InterruptedException("mask shows that tracker " + trackingHostIndex
                  + " is found as LINK in site " + trackedHostIndex + " but it is not");
            }
          } catch (Exception ex) {
            continue;
          }// end try
        }// end loop
      }
    }

    private Collection<TrackerWithType> createTrackersWithType(List<String> trackers, TrackingType type) {
      List<TrackerWithType> trackerWithTypeList = new ArrayList<TrackerWithType>(trackers.size());
      for (String tracker : trackers)
        trackerWithTypeList.add(new TrackerWithType(tracker, 0, type));
      return trackerWithTypeList;
    }
  }
}
