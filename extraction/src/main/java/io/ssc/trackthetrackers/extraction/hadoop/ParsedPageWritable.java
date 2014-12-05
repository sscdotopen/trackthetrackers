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

import io.ssc.trackthetrackers.extraction.proto.ParsedPageProtos;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//TODO is there a way to avoid the intermediate byte arrays here?
public class ParsedPageWritable implements Writable {

  private ParsedPageProtos.ParsedPage parsedPage;

  public ParsedPageProtos.ParsedPage getParsedPage() {
    return parsedPage;
  }

  public void setParsedPage(ParsedPageProtos.ParsedPage parsedPage) {
    this.parsedPage = parsedPage;
  }

  @Override
  public void write(DataOutput out) throws IOException {

    byte[] bytes = parsedPage.toByteArray();

    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    byte[] bytes = new byte[in.readInt()];

    in.readFully(bytes);

    parsedPage = ParsedPageProtos.ParsedPage.parseFrom(bytes);
  }
}
