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

package io.ssc.trackthetrackers.input.mapred;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.hadoopcompatibility.mapred.HadoopInputFormat;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Pattern;


public class GroupByCompany {
   
  public static void run(String input) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // Set up the Hadoop Input Format
    HadoopInputFormat<Text, Text> hadoopInputFormat = 
        new HadoopInputFormat<Text, Text>(new SequenceFileInputFormat(), Text.class, Text.class, new JobConf());
    SequenceFileInputFormat.addInputPath(hadoopInputFormat.getJobConf(), new Path(input));


    DataSet<Tuple2<Text, Text>> data = env.createInput(hadoopInputFormat);

    DataSet<Tuple3<Integer, String, Long>> red = data.flatMap(new Tokenizer(getDictionary())).groupBy(1).aggregate(Aggregations.SUM,2);

    //print top 100 tracker
    red.groupBy(0).sortGroup(2,Order.DESCENDING).first(100).project(1,2).types(String.class, Long.class).print();

    env.execute("Tracker Count");
  }
  
  
  public static HashMap<String, String> getDictionary() {
    HashMap<String, String> dictionary = new HashMap<String, String>();
    dictionary.put("google", "Google");
    dictionary.put("doubleclick", "Google");
    dictionary.put("youtube", "Google"); //is this really a tracker?
    
    dictionary.put("facebook", "Facebook");
    dictionary.put("fbcdn", "Facebook");
    
    dictionary.put("twitter", "Twitter");
    dictionary.put("scorecardresearch", "ScorecardResearch");
    dictionary.put("quantserve", "Quantcast");
    dictionary.put("addthis", "AddThis");

    dictionary.put("chartbeat", "Chartbeat");

    dictionary.put("disqus", "Disqus");
    
    return dictionary;
  }

  public static class Tokenizer implements FlatMapFunction<Tuple2<Text,Text>, Tuple3<Integer,String,Long>> {
    private final Pattern SEP = Pattern.compile(",");

    private HashMap<String,String> dictionary;

    public Tokenizer(HashMap<String,String> dictionary) {
      this.dictionary = dictionary;
    }
    
    @Override
    public void flatMap(Tuple2<Text,Text> value, Collector<Tuple3<Integer,String,Long>> out) {
      String[] allWatchers = SEP.split(value.f1.toString());
      
      HashSet<String> distinctTrackerCompanies = new HashSet<String>();

      for (String aWatcher : allWatchers) {
        boolean found = false;
        for (Map.Entry<String,String> entry : dictionary.entrySet()) {
          if (aWatcher.contains(entry.getKey())) {
            distinctTrackerCompanies.add(entry.getValue());
            found = true;
            break;
          }
        }
        if (!found) {
          distinctTrackerCompanies.add(aWatcher);
        }
      }
      
      for (String aWatcher : distinctTrackerCompanies) {
        out.collect(new Tuple3<Integer,String,Long>(0,aWatcher,1L));
      }
    }
  }
  
  
  
}
