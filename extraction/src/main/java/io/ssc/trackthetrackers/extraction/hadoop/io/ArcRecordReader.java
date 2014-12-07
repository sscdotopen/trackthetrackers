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

package io.ssc.trackthetrackers.extraction.hadoop.io;

import java.io.EOFException;
import java.io.IOException;

import java.lang.Math;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads ARC records.
 * 
 * Set "io.file.buffer.size" to define the amount of data that should be
 * buffered from S3.
 */
public class ArcRecordReader extends RecordReader<Text, ArcRecord> {

  private static final Logger log = LoggerFactory.getLogger(ArcRecordReader.class);

  private GzipCompressorInputStream gzipIn;
  private long fileLength;

  private Text key;
  private ArcRecord value = new ArcRecord();

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    FileSplit split = (FileSplit) genericSplit;
    if (split.getStart() != 0) {
      //TODO: check this
      /*
      IOException ex = new IOException("Invalid ARC file split start " + split.getStart()
          + ": ARC files are not splittable");
      log.error(ex.getMessage(), ex);
      throw ex;
      */
    }

    // open the file and seek to the start of the split
    final Path file = split.getPath();

    FileSystem fs = file.getFileSystem(context.getConfiguration());

    // create a GZIP stream that *does not* automatically read through members
    gzipIn = new GzipCompressorInputStream(fs.open(file), false);

    fileLength = fs.getFileStatus(file).getLen();

    // First record should be an ARC file header record.  Skip it.
    skipRecord();
  }

  /**
   * Skips the current record, and advances to the next GZIP member.
   */
  private void skipRecord() throws IOException {

    long n = 0;

    do {
      n = gzipIn.skip(999999999);
    } while (n > 0);

    gzipIn.nextMember();
  }

  private static byte[] checkBuffer = new byte[64];

  @Override
  public Text getCurrentKey() throws IOException,InterruptedException {
    return key;
  }

  @Override
  public ArcRecord getCurrentValue() throws IOException, InterruptedException {
    System.out.println(value);
    return value;
  }

  public synchronized boolean nextKeyValue() throws IOException, InterruptedException {

    boolean isValid = true;

    if (key == null) {
      key = new Text();
    }
    if (value == null) {
      value = new ArcRecord();
    }
    
    // try reading an ARC record from the stream
    try {
      isValid = value.readFrom(gzipIn);
    } catch (EOFException ex) {
      key = null;
      value = null;
      return false;
    }

    // if the record is not valid, skip it
    if (!isValid) {
      log.error("Invalid ARC record found at GZIP position " + this.gzipIn.getBytesRead() + ".  Skipping ...");
      skipRecord();
      return true;
    }

    if (value.getURL() != null) {
      key.set(value.getURL());
    }

    // check to make sure we've reached the end of the GZIP member
    int n = gzipIn.read(checkBuffer, 0, 64);

    if (n != -1) {
      log.error(n + "  bytes of unexpected content found at end of ARC record.  Skipping ...");
      skipRecord();
    }
    else {
      gzipIn.nextMember();
    }
   
    return true;
  }

  public float getProgress() throws IOException {
    return Math.min(1.0f, gzipIn.getBytesRead() / (float) fileLength);
  }

  public synchronized long getPos() throws IOException {
    return gzipIn.getBytesRead();
  }

  public synchronized void close() throws IOException {
    if (gzipIn != null) {
      gzipIn.close();
    }
  }

}
