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

package io.ssc.trackthetrackers.extraction.resources;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Resource implements Writable {

  private String url;
  private Type type;

  public enum Type {
    LINK, IMAGE, SCRIPT, IFRAME, OTHER
  }

  public Resource() {}

  public Resource(String url, Type type) {
    this.url = Preconditions.checkNotNull(url);
    this.type = Preconditions.checkNotNull(type);
  }

  public String url() {
    return url;
  }

  public Type type() {
    return type;
  }

  @Override
  public int hashCode() {
    return 31 * type.hashCode() + url.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Resource) {
      Resource other = (Resource) obj;
      return type.equals(other.type) && url.equals(other.url);
    }
    return false;
  }

  @Override
  public String toString() {
    return "Resource [" + url + ", " + type + "]";
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(type.ordinal());
    out.writeUTF(url);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type = Type.values()[in.readInt()];
    url = in.readUTF();
  }
}