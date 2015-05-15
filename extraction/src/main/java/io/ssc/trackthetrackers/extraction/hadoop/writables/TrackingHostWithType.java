/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz, Karim Wadie
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

package io.ssc.trackthetrackers.extraction.hadoop.writables;

import com.google.common.base.Preconditions;
import io.ssc.trackthetrackers.extraction.resources.TrackingType;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TrackingHostWithType implements Writable {

  private String trackingDomain;
  private TrackingType type;

  public TrackingHostWithType() {}

  public TrackingHostWithType(String trackingDomain, TrackingType type) {
    this.trackingDomain = Preconditions.checkNotNull(trackingDomain);
    this.type = Preconditions.checkNotNull(type);
  }

  public String trackingDomain() {
    return trackingDomain;
  }

  public TrackingType type() {
    return type;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(trackingDomain);
    out.writeInt(type.ordinal());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    trackingDomain = in.readUTF();
    type = TrackingType.values()[in.readInt()];
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TrackingHostWithType that = (TrackingHostWithType) o;

    if (!trackingDomain.equals(that.trackingDomain)) return false;
    return type == that.type;

  }

  @Override
  public int hashCode() {
    int result = trackingDomain.hashCode();
    result = 31 * result + type.hashCode();
    return result;
  }
}
