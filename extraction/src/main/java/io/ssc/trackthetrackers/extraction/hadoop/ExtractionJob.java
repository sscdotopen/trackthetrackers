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

import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;

import io.ssc.trackthetrackers.extraction.hadoop.io.ArcInputFormat;
import io.ssc.trackthetrackers.extraction.hadoop.io.ArcRecord;
import io.ssc.trackthetrackers.extraction.resources.ResourceExtractor;
import io.ssc.trackthetrackers.extraction.resources.Resource;
import io.ssc.trackthetrackers.commons.proto.ParsedPageProtos;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.util.ToolRunner;
import org.apache.http.HttpResponse;
import org.apache.http.HttpException;
import org.apache.http.ParseException;
import org.apache.http.ProtocolException;
import org.apache.http.entity.ContentType;

import parquet.hadoop.metadata.CompressionCodecName;
import parquet.proto.ProtoParquetOutputFormat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExtractionJob extends HadoopJob {

  public enum JobCounters {
    PAGES, RESOURCES, PROTOCOL_EXCEPTIONS, HTTP_EXCEPTIONS, PARSE_EXCEPTIONS, CHARSET_EXCEPTIONS, STACKOVERFLOW_ERRORS,
    EXTRACTIONS_KILLED
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractionJob(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path[] inputPaths = inputPaths(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job job = mapOnly(inputPaths, outputPath, ArcInputFormat.class, ProtoParquetOutputFormat.class,
                      CommonCrawlExtractionMapper.class, null, null);

    ProtoParquetOutputFormat.setProtobufClass(job, ParsedPageProtos.ParsedPage.class);
    ProtoParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    ProtoParquetOutputFormat.setEnableDictionary(job, true);

    job.waitForCompletion(true);

    return 0;
  }

  /** custom ThreadFactory which allows explicit stopping of created threads for deadlock resolution */
  static class RobustThreadFactory implements ThreadFactory {

    private final ThreadFactory delegate = Executors.defaultThreadFactory();

    private final List<Thread> threadsCreated = new ArrayList<Thread>();

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = delegate.newThread(runnable);
      threadsCreated.add(thread);
      return thread;
    }

    /*ugly, but necessary as the google JS parser sometimes deadlocks... */
    void killDeadlockedThreads() {
      for (Thread thread : threadsCreated) {
        if (thread.isAlive()) {
          thread.interrupt();
          thread.stop();
        }
      }
    }
  }

  static class CommonCrawlExtractionMapper extends Mapper<Writable, ArcRecord, Void, ParsedPageProtos.ParsedPage> {

    private final ResourceExtractor resourceExtractor = new ResourceExtractor();

    private final RobustThreadFactory threadFactory = new RobustThreadFactory();
    private ExecutorService executorService = Executors.newSingleThreadExecutor(threadFactory);

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      executorService.shutdownNow();
      threadFactory.killDeadlockedThreads();
    }

    @Override
    public void map(Writable key, final ArcRecord record, Context context) throws IOException, InterruptedException {

      if ("text/html".equals(record.getContentType())) {
        Charset charset = null;

        try {
          HttpResponse httpResponse = record.getHttpResponse();
          // Default value returned is "html/plain" with charset of ISO-8859-1.
          try {
            charset = ContentType.getOrDefault(httpResponse.getEntity()).getCharset();
          } catch (ParseException e) {
            context.getCounter(JobCounters.PARSE_EXCEPTIONS).increment(1);
          } catch (UnsupportedCharsetException uce) {
            context.getCounter(JobCounters.CHARSET_EXCEPTIONS).increment(1);
          } catch (IllegalCharsetNameException cne) {
            context.getCounter(JobCounters.CHARSET_EXCEPTIONS).increment(1);
          }

          // if anything goes wrong, try ISO-8859-1
          if (charset == null) {
            charset = Charset.forName("ISO-8859-1");
          }

          final String html;
          InputStreamReader reader = null;
          try {
            reader = new InputStreamReader(httpResponse.getEntity().getContent(), charset);
            html = CharStreams.toString(reader);
          } finally {
            Closeables.close(reader, true);
          }

          Future<Iterable<Resource>> futureResources = executorService.submit(new Callable<Iterable<Resource>>() {
            @Override
            public Iterable<Resource> call() throws Exception {
              return resourceExtractor.extractResources(record.getURL(), html);
            }
          });

          Iterable<Resource> resources = futureResources.get(3, TimeUnit.SECONDS);

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
        } catch (StackOverflowError soe) {
          context.getCounter(JobCounters.STACKOVERFLOW_ERRORS).increment(1);
        } catch (ExecutionException e) {
          context.getCounter(JobCounters.EXTRACTIONS_KILLED).increment(1);
          robustExecutorServiceRest();
        } catch (TimeoutException e) {
          context.getCounter(JobCounters.EXTRACTIONS_KILLED).increment(1);
          robustExecutorServiceRest();
        }
      }
    }

    private void robustExecutorServiceRest() {
      executorService.shutdownNow();
      threadFactory.killDeadlockedThreads();
      executorService = Executors.newSingleThreadExecutor(threadFactory);
    }
  }


}
