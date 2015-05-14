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

  protected Path[] inputPaths(String arg) {
    String[] tokens = arg.split(",");
    Path[] paths = new Path[tokens.length];
    for (int pos = 0; pos < tokens.length; pos++) {
      paths[pos] = new Path(tokens[pos]);
    }
    return paths;
  }

  protected Job mapOnly(Path[] input, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
                        Class keyClass, Class valueClass) throws IOException {

    Job job = map(input, output, inputFormatClass, outputFormatClass, mapperClass, keyClass, valueClass);

    job.setNumReduceTasks(0);
    return job;
  }

  protected Job mapOnly(Path input, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
                        Class keyClass, Class valueClass) throws IOException {

    Job job = map(new Path[] { input }, output, inputFormatClass, outputFormatClass, mapperClass, keyClass, valueClass);

    job.setNumReduceTasks(0);
    return job;
  }

  private Job map(Path[] inputPaths, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
                  Class keyClass, Class valueClass) throws IOException {

    Configuration conf = new Configuration();

    Job job = new Job(conf, mapperClass.getSimpleName());

    job.setJarByClass(getClass());

    job.setMapperClass(mapperClass);
    if (keyClass != null && valueClass != null) {
      job.setMapOutputKeyClass(keyClass);
      job.setMapOutputValueClass(valueClass);
    }

    for (Path input : inputPaths) {
      FileInputFormat.addInputPath(job, input);
    }

    job.setInputFormatClass(inputFormatClass);

    job.setOutputFormatClass(outputFormatClass);
    FileOutputFormat.setOutputPath(job, output);

    return job;
  }


  protected Job mapReduce(Path input, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
                          Class mapperKeyClass, Class mapperValueClass, Class reducerClass, Class reducerKeyClass,
                          Class reducerValueClass, boolean combinable) throws IOException {

    return mapReduce(new Path[] { input }, output, inputFormatClass, outputFormatClass, mapperClass, mapperKeyClass,
        mapperValueClass, reducerClass, reducerKeyClass, reducerValueClass, combinable);
  }

  protected Job mapReduce(Path[] inputs, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
                          Class mapperKeyClass, Class mapperValueClass, Class reducerClass, Class reducerKeyClass,
                          Class reducerValueClass, boolean combinable) throws IOException {

    Job job = map(inputs, output, inputFormatClass, outputFormatClass, mapperClass, mapperKeyClass,
        mapperValueClass);

    job.setReducerClass(reducerClass);
    job.setOutputKeyClass(reducerKeyClass);
    job.setOutputValueClass(reducerValueClass);

    if (combinable) {
      job.setCombinerClass(reducerClass);
    }

    return job;
  }
}
