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


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntArrayWritable implements Writable {

  private int[] values;

  public IntArrayWritable() {}

  public IntArrayWritable(int[] values) {
    this.values = values;
  }

  public int[] values() {
    return values;
  }

  public void set(int[] values) {
    this.values = values;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(values.length);
    for (int value : values) {
      out.writeInt(value);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    values = new int[in.readInt()];
    for (int n = 0; n < values.length; n++) {
      values[n] = in.readInt();
    }
  }
}
