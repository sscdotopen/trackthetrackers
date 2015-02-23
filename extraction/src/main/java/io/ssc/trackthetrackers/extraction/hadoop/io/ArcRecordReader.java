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

// modified version of https://github.com/norvigaward/2012-naward15/blob/master/src/cwi/arcUtils/ArcRecordReader.java

package io.ssc.trackthetrackers.extraction.hadoop.io;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArcRecordReader extends RecordReader<Text, ArcRecord> {

  private static final Logger log = LoggerFactory.getLogger(ArcRecordReader.class);

  private FSDataInputStream fsin;
  private GzipCompressorInputStream gzip;
  private long fileLength;
  private Text key;
  private ArcRecord value;
  private Configuration conf;

  public void initialize(InputSplit insplit, TaskAttemptContext context) throws IOException {

    conf = context.getConfiguration();

    FileSplit split = (FileSplit) insplit;

    if (split.getStart() != 0) {
      String errorMessage = "Invalid ARC file split start " + split.getStart() + ": ARC files are not splittable";
      log.error(errorMessage);
      throw new IOException(errorMessage);
    }

    // open the file and seek to the start of the split
    final Path file = split.getPath();

    FileSystem fs = file.getFileSystem(context.getConfiguration());

    fsin = fs.open(file);

    // create a GZIP stream that *does not* automatically read through
    // members
    gzip = new GzipCompressorInputStream(fsin, false);

    fileLength = fs.getFileStatus(file).getLen();

    // First record should be an ARC file header record. Skip it.
    skipRecord();
  }

  /**
   * Skips the current record, and advances to the next GZIP member.
   */
  private void skipRecord() throws IOException {

    long n = 0;

    do {
      n = gzip.skip(999999999);
    } while (n > 0);

    gzip.nextMember();
  }

  public Text createKey() {
    return new Text();
  }

  public ArcRecord createValue() {
    return new ArcRecord();
  }

  private static byte[] checkBuffer = new byte[64];

  /**
   *
   */
  public synchronized boolean nextKeyValue() throws IOException, InterruptedException {

    boolean isValid = true;

    key = (Text) ReflectionUtils.newInstance(Text.class, conf);
    value = (ArcRecord) ReflectionUtils.newInstance(ArcRecord.class, conf);

    // try reading an ARC record from the stream
    try {
      isValid = value.readFrom(gzip);
    } catch (EOFException ex) {
      return false;
    }

    // if the record is not valid, skip it
    if (isValid == false) {
      log.error("Invalid ARC record found at GZIP position " + gzip.getBytesRead() + ".  Skipping ...");
      skipRecord();
      return true;
    }

    if (value.getURL() != null) {
      key.set(value.getURL());
    }

    // check to make sure we've reached the end of the GZIP member
    int n = gzip.read(checkBuffer, 0, 64);

    if (n != -1) {
      log.error(n + "  bytes of unexpected content found at end of ARC record.  Skipping ...");
      skipRecord();
    } else {
      gzip.nextMember();
    }

    return true;
  }

  public float getProgress() throws IOException {
    return Math.min(1.0f, gzip.getBytesRead() / (float) fileLength);
  }

  public synchronized long getPos() throws IOException {
    return gzip.getBytesRead();
  }

  public synchronized void close() throws IOException {
    if (gzip != null) {
      gzip.close();
    }
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public ArcRecord getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

}
