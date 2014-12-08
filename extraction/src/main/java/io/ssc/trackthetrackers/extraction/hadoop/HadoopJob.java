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

package io.ssc.trackthetrackers.extraction.hadoop;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

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

  protected JobConf mapOnly(Path input, Path output, Class inputFormatClass, Class outputFormatClass, Class mapperClass,
                            Class keyClass, Class valueClass) {
    JobConf conf = new JobConf(getClass());
    conf.setJobName(mapperClass.getSimpleName());

    conf.setNumReduceTasks(0);

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

  protected JobConf mapReduce(Path input, Path output, Class inputFormatClass, Class outputFormatClass,
                              Class mapperClass, Class mapperKeyClass, Class mapperValueClass,
                              Class reducerClass, Class reducerKeyClass, Class reducerValueClass) {
    JobConf conf = new JobConf(getClass());
    conf.setJobName(mapperClass.getSimpleName() + "-" + reducerClass.getSimpleName());

    FileOutputFormat.setOutputPath(conf, output);
    FileOutputFormat.setCompressOutput(conf, true);

    FileInputFormat.addInputPath(conf, input);
    conf.setInputFormat(inputFormatClass);
    conf.setOutputFormat(outputFormatClass);

    conf.setMapperClass(mapperClass);
    conf.setMapOutputValueClass(mapperValueClass);

    conf.setReducerClass(reducerClass);
    conf.setOutputKeyClass(reducerKeyClass);
    conf.setOutputValueClass(reducerValueClass);

    return conf;
  }

}
