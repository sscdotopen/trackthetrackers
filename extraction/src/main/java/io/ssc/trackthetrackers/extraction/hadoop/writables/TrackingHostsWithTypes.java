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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import java.util.Collection;

public class TrackingHostsWithTypes extends ArrayWritable {

  public TrackingHostsWithTypes() {
    super(TrackingHostWithType.class);
  }

  public TrackingHostsWithTypes(Collection<TrackingHostWithType> elements) {
    super(TrackingHostWithType.class);
    set(elements.toArray(new Writable[] {}));
  }

  public TrackingHostWithType[] values() {
    Writable[] writables = get();
    TrackingHostWithType[] values = new TrackingHostWithType[writables.length];
    for (int n = 0; n < writables.length; n++) {
      values[n] = (TrackingHostWithType) writables[n];
    }
    return values;
  }
}
