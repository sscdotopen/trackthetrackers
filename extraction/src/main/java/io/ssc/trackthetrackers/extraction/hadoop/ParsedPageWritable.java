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
