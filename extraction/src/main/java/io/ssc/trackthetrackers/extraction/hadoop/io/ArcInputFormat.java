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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/** A input format the reads arc files. */
public class ArcInputFormat extends FileInputFormat<Text, ArcRecord> {

  /**
   * Returns the <code>RecordReader</code> for reading the arc file.
   * 
   * @param split The InputSplit of the arc file to process.
   * @param job The job configuration.
   * @param reporter The progress reporter.
   */
  public RecordReader<Text, ArcRecord> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    reporter.setStatus(split.toString());
    return new ArcRecordReader(job, (FileSplit)split);
  }

  /**
   * <p>Always returns false to indicate that ARC files are not splittable.</p>
   * <p>ARC files are stored in 100MB files, meaning they will be stored in at
   * most 3 blocks (2 blocks on Hadoop systems with 128MB block size).</p>
   */
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }
}

