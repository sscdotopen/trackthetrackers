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

package io.ssc.trackthetrackers.extraction.hadoop.util;


import io.ssc.trackthetrackers.commons.proto.ParsedPageProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.proto.ProtoParquetInputFormat;
import parquet.proto.ProtoParquetOutputFormat;
import parquet.proto.ProtoReadSupport;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Compaction {

  public static void main(String[] args) throws IOException, InterruptedException {

    Configuration conf = new Configuration();

    FileSystem fs = FileSystem.get(conf);

    int block = 9;

    FileStatus[] input = fs.listStatus(new Path("/home/ssc/Desktop/trackthetrackers/emr/block" + block +"/"),
        new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return path.toString().endsWith(".parquet");
          }
        });

    Path output = new Path("/home/ssc/Desktop/trackthetrackers/compacted/block" + block + ".snappy.parquet");

    fs.delete(output, true);

    ProtoParquetInputFormat<ParsedPageProtos.ParsedPageOrBuilder> inputFormat = new ProtoParquetInputFormat<ParsedPageProtos.ParsedPageOrBuilder>();
    inputFormat.setReadSupportClass(new JobConf(conf), ProtoReadSupport.class);

    Job job = new Job(conf);
    ProtoParquetOutputFormat<ParsedPageProtos.ParsedPage> outputFormat =
        new ProtoParquetOutputFormat<ParsedPageProtos.ParsedPage>(ParsedPageProtos.ParsedPage.class);
    ProtoParquetOutputFormat.setProtobufClass(job, ParsedPageProtos.ParsedPage.class);
    ProtoParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    ProtoParquetOutputFormat.setEnableDictionary(job, true);

    RecordWriter<Void, ParsedPageProtos.ParsedPage> recordWriter =
        outputFormat.getRecordWriter(conf, output, CompressionCodecName.SNAPPY);

    List<ParquetInputSplit> splits = new ArrayList<ParquetInputSplit>();

    for (FileStatus fileStatus : input) {
      System.out.println(fileStatus.getPath().toString());
      splits.addAll(inputFormat.getSplits(conf, ParquetFileReader.readFooters(conf, fileStatus)));
    }



    int splitIndex = 0;
    for (ParquetInputSplit split : splits) {

      System.out.println("Processing split: " + split.getPath().toString() +
                         "(" + splitIndex + " of " +  splits.size() + ")");

      TaskAttemptID taskAttemptID = new TaskAttemptID(new TaskID("identifier", splitIndex, true, splitIndex),
                                                      splitIndex);
      TaskAttemptContext ctx = new org.apache.hadoop.mapreduce.TaskAttemptContext(conf, taskAttemptID);

      RecordReader<Void, ParsedPageProtos.ParsedPageOrBuilder> reader = inputFormat.createRecordReader(split, ctx);
      reader.initialize(split, ctx);

      while (reader.nextKeyValue()) {

        ParsedPageProtos.ParsedPageOrBuilder record = reader.getCurrentValue();

        ParsedPageProtos.ParsedPage.Builder builder = ParsedPageProtos.ParsedPage.newBuilder();

        builder.setUrl(record.getUrl());
        builder.setArchiveTime(record.getArchiveTime());

        builder.addAllScripts(record.getScriptsList());
        builder.addAllIframes(record.getIframesList());
        builder.addAllLinks(record.getLinksList());
        builder.addAllImages(record.getImagesList());

        recordWriter.write(null, builder.build());
      }

      if (reader != null) {
        reader.close();
      }

      splitIndex++;
    }

    TaskAttemptID taskAttemptID = new TaskAttemptID(new TaskID("identifier", 1, true, 1), 1);
    TaskAttemptContext ctx = new org.apache.hadoop.mapreduce.TaskAttemptContext(conf, taskAttemptID);

    if (recordWriter != null) {
      recordWriter.close(ctx);
    }

  }
}
