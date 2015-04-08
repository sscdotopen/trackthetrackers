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
import java.util.HashSet;
import java.util.List;
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
        EdgeListMapper.class, IntWritable.class, TwoDIntArrayWritable.class,
        DistinctifyReducer.class, IntWritable.class, Text.class, false, true);

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
    	
      Map<Integer,String> trackingHosts = new HashMap<Integer,String>();
      int trackedHostIndex = trackedHost.get();
      
      //fill out the trackers and tracking type into a set 
      for (TwoDArrayWritable tdArray : values) {
      	  
    	 Writable [][] array = tdArray.get();    	
    	 for(int i=0; i< array.length; i++){
    		 int trackingHostIndex = ((IntWritable) array[i][0]).get();
    		 int trackingHostType = ((IntWritable) array[i][1]).get();
    		     		 
    		 //don't add a link from the tracked node to itslef
    		 //concatenate the tracking types for each unique tracker-tracked site
    		 if(trackingHostIndex!= trackedHostIndex)
    			 if(trackingHosts.containsKey(trackingHostIndex)){
    				 trackingHosts.put(trackingHostIndex, trackingHosts.get(trackingHostIndex)+ Integer.toString(trackingHostType)+DELIM);
    				 
    			 }else
    				 trackingHosts.put(trackingHostIndex, Integer.toString(trackingHostType)+DELIM ); 				     			 	 
    	 }
   	 
      }
      

      for (Integer trackingHost : trackingHosts.keySet()) {
    	  
    	List tags = Arrays.asList(trackingHosts.get(trackingHost).split(DELIM));
    	System.out.println();
    	String trackingTypeMask = "";
    	
    	trackingTypeMask  += tags.contains( Integer.toString( TrackingTypes.SCRIPT )) ? "1": "0";
    	trackingTypeMask  += tags.contains( Integer.toString( TrackingTypes.IFRAME )) ? "1": "0";
    	trackingTypeMask  += tags.contains( Integer.toString( TrackingTypes.IMAGE )) ? "1": "0";
    	trackingTypeMask  += tags.contains( Integer.toString( TrackingTypes.LINK )) ? "1": "0";

    	/*
    	trackingTypeMask  = tags.contains( Integer.toString( TrackingTypes.SCRIPT )) ? trackingTypeMask+"1": trackingTypeMask+"0";
    	trackingTypeMask  = tags.contains( Integer.toString( TrackingTypes.IFRAME )) ? trackingTypeMask+"1": trackingTypeMask+"0";
    	trackingTypeMask  = tags.contains( Integer.toString( TrackingTypes.IMAGE )) ? trackingTypeMask+"1": trackingTypeMask+"0";
    	trackingTypeMask  = tags.contains( Integer.toString( TrackingTypes.LINK )) ? trackingTypeMask+"1": trackingTypeMask+"0";
    	*/

    	String out = trackedHostIndex + "\t"+ trackingTypeMask ;  
        ctx.write(new IntWritable(trackingHost), new Text(out));
      }
    }
  }


  static class EdgeListMapper extends Mapper<Void, ParsedPageProtos.ParsedPage.Builder, IntWritable, TwoDIntArrayWritable> {

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

            Set<TrackerWithType> allTrackingDomains = Sets.newHashSet();
            
            allTrackingDomains.addAll(createTrackersWithType(parsedPage.getScriptsList(), TrackingTypes.SCRIPT ));
            allTrackingDomains.addAll(createTrackersWithType(parsedPage.getIframesList(),TrackingTypes.IFRAME ));
            allTrackingDomains.addAll(createTrackersWithType(parsedPage.getImagesList(),TrackingTypes.IMAGE));
            allTrackingDomains.addAll(createTrackersWithType(parsedPage.getLinksList(),TrackingTypes.LINK ));

            IntWritable[][] trackingHosts = new IntWritable[allTrackingDomains.size()][2];

            int n = 0;
            for (TrackerWithType trackingDomain : allTrackingDomains) {
              String trackingHost = InternetDomainName.from(trackingDomain.getTrackerDomain()).topPrivateDomain().toString();
              int trackingHostIndex = domainIndex.indexFor(trackingHost);
              trackingHosts[n][0] = new IntWritable(trackingHostIndex);
              trackingHosts[n][1] = new IntWritable(trackingDomain.getTrackerType());
              n++;
            }
            
            TwoDIntArrayWritable out = new TwoDIntArrayWritable();
            out.set(trackingHosts);
         
            ctx.write(new IntWritable(trackedHostIndex), out);

          } catch (Exception e) {
            //TODO counter
          }
        }
      }
    }
  

    private Collection<TrackerWithType> createTrackersWithType(List<String> trackers, int type){
    	
    	List<TrackerWithType> ret = new ArrayList<TrackerWithType>(trackers.size());
    	for(String tracker: trackers)
    		ret.add(new TrackerWithType(tracker, 0 , type));
    	return ret;
    	
    }
  
  }

}
