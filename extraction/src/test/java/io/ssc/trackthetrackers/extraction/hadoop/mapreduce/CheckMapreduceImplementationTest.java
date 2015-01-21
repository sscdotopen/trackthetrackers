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

package io.ssc.trackthetrackers.extraction.hadoop.mapreduce;

import io.ssc.trackthetrackers.extraction.hadoop.Config;
import io.ssc.trackthetrackers.extraction.hadoop.mapred.AggregateScriptWatchersJobMapred;
import io.ssc.trackthetrackers.extraction.hadoop.mapred.ExtractionJobMapred;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CheckMapreduceImplementationTest {

    @Test
    public void testCheckMapreduceImplementation() throws Exception {
        
        ExtractionJob extractionMapreduce = new ExtractionJob();
    
        ToolRunner.run(extractionMapreduce, new String[] {
            "--input", Config.get("commoncrawl.samples.path"),
            "--output", "/tmp/commoncrawl-extraction/mapreduce/"
        });
        
        long numberPages = extractionMapreduce.getCount(ExtractionJob.JobCounters.PAGES);
        long numberResources = extractionMapreduce.getCount(ExtractionJob.JobCounters.RESOURCES);  
        
        
        ExtractionJobMapred extractionMapred = new ExtractionJobMapred();
    
        ToolRunner.run(extractionMapred, new String[] {
              "--input", Config.get("commoncrawl.samples.path"),
              "--output", "/tmp/commoncrawl-extraction/mapred/"
        });
    
        long numberPagesShould = extractionMapred.getCount(ExtractionJobMapred.JobCounters.PAGES);
        long numberResourcesShould = extractionMapred.getCount(ExtractionJobMapred.JobCounters.RESOURCES);
    
        
        assertEquals(numberPages, numberPagesShould);
        assertEquals(numberResources,numberResourcesShould);
    
    
        AggregateScriptWatchersJob aggregateWatchers = new AggregateScriptWatchersJob();
        ToolRunner.run(aggregateWatchers, new String[] {
                "--input", "/tmp/commoncrawl-extraction/mapreduce/",
                "--output", "/tmp/commoncrawl-watchers/mapreduce/"
        });
    
        long numberPagesAggregation = aggregateWatchers.getCount(AggregateScriptWatchersJob.JobCounters.PAGES);
        long numberScriptsAggregation = aggregateWatchers.getCount(AggregateScriptWatchersJob.JobCounters.SCRIPTS);
    
        assertEquals(numberPages, numberPagesAggregation);    
    
        AggregateScriptWatchersJobMapred aggregateWatchersMapred = new AggregateScriptWatchersJobMapred();
        ToolRunner.run(aggregateWatchersMapred, new String[] {
                "--input", "/tmp/commoncrawl-extraction/mapred/",
                "--output", "/tmp/commoncrawl-watchers/mapred/"
        });
    
        long numberPagesAggregationShould = aggregateWatchersMapred.getCount(AggregateScriptWatchersJobMapred.JobCounters.PAGES);
        long numberResourcesAggregation = aggregateWatchersMapred.getCount(AggregateScriptWatchersJobMapred.JobCounters.RESOURCES);
            
        System.out.println("MapReduce Pages: " + numberPagesAggregation);
        System.out.println("MapRed Pages: " + numberPagesAggregationShould);

        System.out.println("MapReduce Scripts: " + numberScriptsAggregation);
        System.out.println("MapRed Resources: " + numberResourcesAggregation);
        
        assertEquals(numberPagesAggregation, numberPagesAggregationShould);

    }
}
