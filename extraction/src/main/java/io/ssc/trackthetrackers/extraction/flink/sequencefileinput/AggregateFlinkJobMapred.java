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

package io.ssc.trackthetrackers.extraction.flink.sequencefileinput;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.mapred.HadoopInputFormat;

import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import java.util.regex.Pattern;


public class AggregateFlinkJobMapred {

   
    public static void run(String input) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Set up the Hadoop Input Format
        HadoopInputFormat<Text, Text> hadoopInputFormat = 
            new HadoopInputFormat<Text, Text>(new SequenceFileInputFormat(), Text.class, Text.class, new JobConf());
        SequenceFileInputFormat.addInputPath(hadoopInputFormat.getJobConf(), new Path(input));


        DataSet<Tuple2<Text, Text>> data = env.createInput(hadoopInputFormat);

        DataSet<Tuple2<String, Long>> reduced = data.flatMap(new Tokenizer()).groupBy(0).aggregate(Aggregations.SUM,1);
        
        reduced.groupBy(0).sortGroup(1,Order.ASCENDING).first(10).print(); //something is wrong here
        
        env.execute("Word Count");
    }

    public static class Tokenizer implements FlatMapFunction<Tuple2<Text,Text>, Tuple2<String,Long>> {
        private final Pattern SEP = Pattern.compile(",");
        
        @Override
        public void flatMap(Tuple2<Text,Text> value, Collector<Tuple2<String,Long>> out) {
            String[] allWatchers = SEP.split(value.f1.toString());
            for (String aWatcher : allWatchers) {
                out.collect(new Tuple2<String, Long>(aWatcher,1L));
            }
        }
    }
}
