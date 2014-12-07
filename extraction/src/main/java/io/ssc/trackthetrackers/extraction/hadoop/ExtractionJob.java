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

import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;

import io.ssc.trackthetrackers.extraction.hadoop.io.ArcInputFormat;
import io.ssc.trackthetrackers.extraction.hadoop.io.ArcRecord;
import io.ssc.trackthetrackers.extraction.resources.Resource;
import io.ssc.trackthetrackers.extraction.resources.ResourceExtractor;

import io.ssc.trackthetrackers.extraction.thrift.ParsedPage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.entity.ContentType;

import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.thrift.ParquetThriftOutputFormat;


import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class ExtractionJob extends HadoopJob {

  public static enum Counters {
    PAGES, RESOURCES
  }

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));



    Configuration conf = new Configuration();

    FileSystem.get(conf).delete(outputPath, true);

    Job job = new Job(conf, CommonCrawlExtractionMapper.class.getSimpleName());

    job.setJarByClass(ExtractionJob.class);

    job.setMapperClass(CommonCrawlExtractionMapper.class);

    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job,inputPath);
    job.setInputFormatClass(ArcInputFormat.class);


    job.setOutputFormatClass(ParquetThriftOutputFormat.class);
    ParquetThriftOutputFormat.setOutputPath(job, outputPath);
    ParquetThriftOutputFormat.setThriftClass(job, ParsedPage.class);

    ParquetThriftOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    ParquetThriftOutputFormat.setEnableDictionary(job, true);


    job.waitForCompletion(true);

    return 0;
  }

  static class CommonCrawlExtractionMapper extends Mapper<Writable, ArcRecord, Void, ParsedPage> {

    private final ResourceExtractor resourceExtractor = new ResourceExtractor();

    @Override
    public void map(Writable key, ArcRecord record, Context context) throws IOException, InterruptedException
    {

      if ("text/html".equals(record.getContentType())) {
        //System.out.println(record.getURL());

        String charset = null;

        try {
          HttpResponse httpResponse = record.getHttpResponse();
          // Default value returned is "html/plain" with charset of ISO-8859-1.
          try {
            charset = ContentType.getOrDefault(httpResponse.getEntity()).getCharset().name();
          } catch (Exception e) {}

          // if anything goes wrong, try ISO-8859-1
          if (charset == null) {
            charset = "ISO-8859-1";
          }

          String html;
          InputStreamReader reader = new InputStreamReader(httpResponse.getEntity().getContent(), charset);
          try {
            html = CharStreams.toString(reader);
          }
          finally {
            Closeables.close(reader, true);
          }

          Iterable<Resource> resources = resourceExtractor.extractResources(record.getURL(), html);

          /*
          reporter.incrCounter(Counters.PAGES, 1);
          reporter.incrCounter(Counters.RESOURCES, Iterables.size(resources));
          */

          ParsedPage pp = new ParsedPage();

          pp.setUrl(record.getURL());
          pp.setArchiveTime(record.getArchiveDate().getTime());

          for (Resource resource : resources) {
            if (Resource.Type.SCRIPT.equals(resource.type())) {
              pp.addToScripts(resource.url());
            } else if (Resource.Type.IFRAME.equals(resource.type())) {
              pp.addToIframes(resource.url());
            } else if (Resource.Type.LINK.equals(resource.type())) {
              pp.addToLinks(resource.url());
            } else if (Resource.Type.IMAGE.equals(resource.type())) {
              pp.addToImages(resource.url());
            }
          }

          context.write(null, pp);

        } catch (ProtocolException pe) {
          // do nothing here
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
  }
}
