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

package io.ssc.trackthetrackers.extraction.hadoop.mapred;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * a very simple base class for hadoop jobs
 */
public abstract class HadoopJobMapred extends Configured implements Tool {

  protected RunningJob job;

  public HadoopJobMapred() {
    job = null;
  }

  public long getCount (Enum<?> counterType) {
    if (job != null) {
      try {
        Counters counters = job.getCounters();
        Counters.Counter c = counters.findCounter(counterType);
        return c.getValue();
      } catch (IOException e) {
      }
    }
    return 0L;
  }

  protected Map<String,String> parseArgs(String[] args) {
    if (args == null || args.length % 2 != 0) {
      throw new IllegalStateException("Cannot convert args!");
    }

    Map<String,String> parsedArgs = Maps.newHashMap();
    for (int n = 0; n < args.length; n += 2) {
      parsedArgs.put(args[n], args[n+1]);
    }
    return Collections.unmodifiableMap(parsedArgs);
  }

  private JobConf map(Path input, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
                      Class keyClass, Class valueClass, boolean deleteOutputFolder) throws IOException {

    JobConf conf = new JobConf(getClass());
    conf.setJobName(mapperClass.getSimpleName());

    if (deleteOutputFolder) {
      FileSystem.getLocal(conf).delete(output, true);
    }

    FileOutputFormat.setOutputPath(conf, output);
    FileOutputFormat.setCompressOutput(conf, true);

    FileInputFormat.addInputPath(conf, input);
    conf.setInputFormat(inputFormatClass);

    conf.setOutputFormat(outputFormatClass);
    conf.setOutputKeyClass(keyClass);
    conf.setOutputValueClass(valueClass);

    conf.setMapperClass(mapperClass);

    return conf;
  }

  protected void mapOnly(Path input, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass, 
                         Class keyClass, Class valueClass, boolean deleteOutputFolder) throws IOException{
    JobConf conf = map(input, output, inputFormatClass, outputFormatClass, mapperClass, keyClass, valueClass, 
        deleteOutputFolder);

    conf.setNumReduceTasks(0);

    runJob(conf);   
  }
  
  protected void runJob (JobConf conf) {
    try {
      job = JobClient.runJob(conf);
      job.waitForCompletion();
    } catch (IOException e) {
      
    }
  }

  protected void mapReduce(Path input, Path output, Class inputFormatClass, Class outputFormatClass, 
                           Class mapperClass, Class mapperKeyClass, Class mapperValueClass,
                           Class reducerClass, Class reducerKeyClass, Class reducerValueClass, boolean combinable, 
                           boolean deleteOutputFolder) throws IOException{
    
    JobConf conf =  map(input, output, inputFormatClass, outputFormatClass, mapperClass, mapperKeyClass, 
        mapperValueClass, deleteOutputFolder);

    conf.setReducerClass(reducerClass);
    conf.setOutputKeyClass(reducerKeyClass);
    conf.setOutputValueClass(reducerValueClass);
    
    if (combinable) {
      conf.setCombinerClass(reducerClass);
    }

    runJob(conf);
  }

}
