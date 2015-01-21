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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public abstract class HadoopJob extends Configured implements Tool {

  protected Job job;

  public HadoopJob() {
    job = null;
  }
  
  public long getCount (Enum<?> counterType) throws IOException {
    if (job != null) {
      Counters counters = job.getCounters();
      Counter c = counters.findCounter(counterType);
      return c.getValue();
    }
    return 0L;
  }
  
  protected Map<String,String> parseArgs(String[] args) {
    if (args == null || args.length % 2 != 0) {
      throw new IllegalStateException("Cannot convert args!");
    }

    Map<String,String> parsedArgs = Maps.newHashMap();
    for (int n = 0; n < args.length; n += 2) {
      parsedArgs.put(args[n], args[n + 1]);
    }
    return Collections.unmodifiableMap(parsedArgs);
  }


  protected void mapOnly(Path input, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
                         Class keyClass, Class valueClass, boolean deleteOutputFolder) throws IOException {

    map(input, output, inputFormatClass, outputFormatClass, mapperClass, keyClass, valueClass, deleteOutputFolder);

    job.setNumReduceTasks(0);
  }

  private void map(Path input, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
                   Class keyClass, Class valueClass, boolean deleteOutputFolder) throws IOException {

    Configuration conf = new Configuration();

    if (deleteOutputFolder) {
      FileSystem.get(conf).delete(output, true);
    }

    job = new Job(conf, mapperClass.getSimpleName());

    job.setJarByClass(getClass());

    job.setMapperClass(mapperClass);
    if (keyClass != null && valueClass != null) {
      job.setMapOutputKeyClass(keyClass);
      job.setMapOutputValueClass(valueClass);
    }

    FileInputFormat.addInputPath(job,input);
    job.setInputFormatClass(inputFormatClass);

    job.setOutputFormatClass(outputFormatClass);
    FileOutputFormat.setOutputPath(job, output);
  }


  protected void mapReduce(Path input, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
                           Class mapperKeyClass, Class mapperValueClass, Class reducerClass, Class reducerKeyClass,
                           Class reducerValueClass, boolean combinable, boolean deleteOutputFolder) throws IOException {

    map(input, output, inputFormatClass, outputFormatClass, mapperClass, mapperKeyClass, mapperValueClass, 
        deleteOutputFolder);

    job.setReducerClass(reducerClass);
    job.setOutputKeyClass(reducerKeyClass);
    job.setOutputValueClass(reducerValueClass);

    if (combinable) {
      job.setCombinerClass(reducerClass);
    }

    FileOutputFormat.setCompressOutput(job, true);
  }

}
