package io.ssc.trackthetrackers.extraction.hadoop;

import io.ssc.trackthetrackers.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class CommonCrawlProblemIntegrationTest {

  public static void main(String[] args) throws Exception {
    ExtractionJob extraction = new ExtractionJob();

    Path output = new Path("/tmp/commoncrawl-extraction-problematic/");

    FileSystem.get(new Configuration()).delete(output, true);

    ToolRunner.run(extraction, new String[]{
        "--input", Config.get("commoncrawl.problematic.path"),
        "--output", output.toString()
    });
  }

}
