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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

/** A input format the reads arc files. */
public class ArcInputFormat extends FileInputFormat<Text, ArcRecord> {

  /**
   * Returns the <code>RecordReader</code> for reading the arc file.
   *
   */
  public RecordReader<Text,ArcRecord> createRecordReader(InputSplit split, TaskAttemptContext context) 
        throws IOException {
    context.setStatus(split.toString());  
    return new ArcRecordReader();
  }

  /**
   * <p>Always returns false to indicate that ARC files are not splittable.</p>
   * <p>ARC files are stored in 100MB files, meaning they will be stored in at
   * most 3 blocks (2 blocks on Hadoop systems with 128MB block size).</p>
   */
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }
}

