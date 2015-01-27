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

import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;

import io.ssc.trackthetrackers.extraction.hadoop.io.mapreduce.ArcInputFormat;
import io.ssc.trackthetrackers.extraction.hadoop.io.mapreduce.ArcRecord;
import io.ssc.trackthetrackers.extraction.resources.ResourceExtractor;
import io.ssc.trackthetrackers.extraction.resources.Resource;
import io.ssc.trackthetrackers.commons.proto.ParsedPageProtos;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.http.HttpResponse;
import org.apache.http.HttpException;
import org.apache.http.ParseException;
import org.apache.http.ProtocolException;
import org.apache.http.entity.ContentType;

import parquet.hadoop.metadata.CompressionCodecName;
import parquet.proto.ProtoParquetOutputFormat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;

public class ExtractionJob extends HadoopJob {

  public static enum JobCounters {
    PAGES, RESOURCES, PROTOCOL_EXCEPTIONS, HTTP_EXCEPTIONS, PARSE_EXCEPTIONS, CHARSET_EXCEPTIONS
  }

  public static void main(String[] args) throws Exception {
    ExtractionJob current = new ExtractionJob();
    current.run(args);
  }
  
  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    mapOnly(inputPath, outputPath, ArcInputFormat.class, ProtoParquetOutputFormat.class, 
        CommonCrawlExtractionMapper.class, null, null, true);

    ProtoParquetOutputFormat.setProtobufClass(job, ParsedPageProtos.ParsedPage.class);
    ProtoParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    ProtoParquetOutputFormat.setEnableDictionary(job, true);

    job.waitForCompletion(true);

    return 0;
  }  

  static class CommonCrawlExtractionMapper extends Mapper<Writable, ArcRecord, Void, ParsedPageProtos.ParsedPage> {

    private final ResourceExtractor resourceExtractor = new ResourceExtractor();

    @Override
    public void map(Writable key, ArcRecord record, Context context) throws IOException, InterruptedException {

      if ("text/html".equals(record.getContentType())) {
        String charset = null;

        try {
          HttpResponse httpResponse = record.getHttpResponse();
          // Default value returned is "html/plain" with charset of ISO-8859-1.
          try {
            if (ContentType.getOrDefault(httpResponse.getEntity()).getCharset() != null) {
              charset = ContentType.getOrDefault(httpResponse.getEntity()).getCharset().name();
            }
          } catch (ParseException e) {
            context.getCounter(JobCounters.PARSE_EXCEPTIONS).increment(1);
          } catch (UnsupportedCharsetException uce) {
            context.getCounter(JobCounters.CHARSET_EXCEPTIONS).increment(1);
          }

          // if anything goes wrong, try ISO-8859-1
          if (charset == null) {
            charset = "ISO-8859-1";
          }

          String html;
          InputStreamReader reader = new InputStreamReader(httpResponse.getEntity().getContent(), charset);
          try {
            html = CharStreams.toString(reader);
          } finally {
            Closeables.close(reader, true);
          }

          Iterable<Resource> resources = resourceExtractor.extractResources(record.getURL(), html);

          context.getCounter(JobCounters.PAGES).increment(1);
          context.getCounter(JobCounters.RESOURCES).increment(Iterables.size(resources));

          ParsedPageProtos.ParsedPage.Builder builder = ParsedPageProtos.ParsedPage.newBuilder();

          builder.setUrl(record.getURL())
                 .setArchiveTime(record.getArchiveDate().getTime());

          for (Resource resource : resources) {
            if (Resource.Type.SCRIPT.equals(resource.type())) {
              builder.addScripts(resource.url());
            } else if (Resource.Type.IFRAME.equals(resource.type())) {
              builder.addIframes(resource.url());
            } else if (Resource.Type.LINK.equals(resource.type())) {
              builder.addLinks(resource.url());
            } else if (Resource.Type.IMAGE.equals(resource.type())) {
              builder.addImages(resource.url());
            }
          }

          context.write(null, builder.build());

        } catch (ProtocolException pe) {
          context.getCounter(JobCounters.PROTOCOL_EXCEPTIONS).increment(1);
        } catch (HttpException e) {
          context.getCounter(JobCounters.HTTP_EXCEPTIONS).increment(1);
          throw new IOException(e);
        }
      }
    }
  }
}
