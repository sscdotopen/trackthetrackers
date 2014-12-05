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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Integer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.io.Writable;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.io.AbstractSessionInputBuffer;
import org.apache.http.impl.io.DefaultHttpResponseParser;
import org.apache.http.message.BasicLineParser;
import org.apache.http.params.BasicHttpParams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An entry in an ARC (Internet Archive) data file.
 *
 * @author Chris Stephens
 */
public class ArcRecord implements Writable {

  private static final Logger LOG = LoggerFactory.getLogger(ArcRecord.class);

  // ARC v1 metadata
  private String url;
  private String ipAddress;
  private Date archiveDate;
  private String contentType;
  private int contentLength;

  private byte[] payload;

  private HttpResponse httpResponse;

  public ArcRecord() { }

  private void clear() {
    this.url = null;
    this.ipAddress = null;
    this.archiveDate = null;
    this.contentType = null;
    this.contentLength = 0;
    this.payload = null;
    this.httpResponse = null;
  }

  private String readLine(InputStream in) throws IOException {

    StringBuilder line = new StringBuilder(128);

    // read a line of content
    int b = in.read();

    // if -1 is returned, we are at EOF
    if (b == -1) {
      throw new EOFException();
    }

    // read until an NL
    do {

      if (((char) b) == '\n') {
        break;
      }

      line.append((char) b);

      b = in.read();
    } while (b != -1);

    return line.toString();
  }

  /**
   * <p>Parses the ARC record header and payload (content) from a stream.</p>
   *
   * @return TRUE if the ARC record was parsed and loaded successfully, FALSE if not.
   */
  public boolean readFrom(InputStream in) throws IOException {

    if (in == null) {
      LOG.error("ArcRecord cannot be created from NULL/missing input stream.");
      return false;
    }

    // Clear any current values assigned to the object.
    clear();

    // Read the ARC header from the stream.
    String arcRecordHeader = readLine(in);

    try {
      setArcRecordHeader(arcRecordHeader);
      setPayload(in);
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      LOG.error("Exception thrown while parsing ARC record", ex);
      return false;
    }
     
    return true;
  }

  /**
   * <p>Parses and sets the ARC record header fields.</p>
   * <p>Currently, this method expects the ARC record header string to contain
   * the following fields, in order, separated by space:
   * <ul>
   * <li>URL</li>
   * <li>IP Address</li>
   * <li>Archive Date</li>
   * <li>Content Type</li>
   * <li>Content Length</li>
   * </ul>
   * </p>
   * <p>For more information on the arc file format, see
   * {@see http://www.archive.org/web/researcher/ArcFileFormat.php}.</p>
   *
   * @param arcRecordHeader The first line of an ARC file entry - the header
   *                        line for an ARC file item.
   */
  public void setArcRecordHeader(String arcRecordHeader) throws IllegalArgumentException, ParseException {

    if (arcRecordHeader == null || arcRecordHeader.equals("")) {
      throw new IllegalArgumentException("ARC v1 record header string is empty.");
    }

    String[] metadata = arcRecordHeader.split(" ");

    if (metadata.length != 5) {
      LOG.info(" [ "+arcRecordHeader+" ] ");
      throw new IllegalArgumentException("ARC v1 record header must be 5 fields.");
    }

    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

    url =  metadata[0];
    ipAddress =  metadata[1];
    archiveDate =  format.parse(metadata[2]);
    contentType =  metadata[3];
    contentLength = new Integer(metadata[4]);
  }

  /**
   * <p>Reads and sets the ARC record payload from an input stream.</p>
   *
   * @param in An input stream positioned at the start of the ARC record payload.
   */
  public void setPayload(InputStream in) throws IllegalArgumentException, ParseException, IOException {

    if (in == null) {
      throw new IllegalArgumentException("ArcRecord cannot be created from NULL/missing input stream.");
    }

    int bufferSize = this.contentLength;

    this.payload = new byte[bufferSize];

    int n = in.read(this.payload, 0, this.payload.length);

    if (n < this.payload.length) {
      LOG.warn("Expecting "+bufferSize+" bytes in ARC record payload, found "+n+" bytes.  Performing array copy.");
      this.payload = Arrays.copyOf(this.payload, n);
    }

    // After this, we should be at the end of this GZIP member.  Let the
    // calling function verify the position of the stream.
  }

  public void addToPayload(byte[] data, int length) {

    LOG.warn("Content Length must have been incorrect - someone needed to add more data to the payload.");

    if (payload == null) {
      payload = Arrays.copyOf(data, length);
    }
    else {
      int i = payload.length;
      int n = payload.length + length;

      // resize the payload buffer
      this.payload = Arrays.copyOf(this.payload, n);

      // copy in the additional data
      System.arraycopy(data, 0, this.payload, i, length);
    }
  }

  public String toString() {
    return this.url + " - " + this.archiveDate.toString() + " - " + this.contentType;
  }

  public void write(DataOutput out)
      throws IOException {

    // write out ARC header info
    out.writeUTF(url);
    out.writeUTF(ipAddress);
    out.writeUTF(contentType);
    out.writeLong(archiveDate.getTime());
    out.writeInt(contentLength);

    // write out the payload
    out.writeInt(payload.length);
    out.write(payload, 0, payload.length);
  }

  public void readFields(DataInput in) throws IOException {

    // read in ARC header info
    url = in.readUTF();
    ipAddress = in.readUTF();
    contentType = in.readUTF();
    archiveDate = new Date(in.readLong());
    contentLength = in.readInt();

    // read in the payload
    int payloadLength = in.readInt();

    // resize the payload buffer if necessary
    if (payload == null || payload.length != payloadLength) {
      payload = new byte[payloadLength];
    }

    try {
      in.readFully(payload, 0, payloadLength);
    } catch (EOFException ex) {
      throw new IOException("End of input reached before payload was fully deserialized.");
    }

    // assume that if a new payload was loaded, HTTP response will need to be reparsed.
    httpResponse = null;
  }

  /**
   * <p>Returns the full ARC record payload.  This is usually a complete HTTP
   * response.</p>
   *
   * @return The raw ARC record content.
   */
  public byte[] getPayload() {
    return payload;
  }

  /**
   * <p>Returns the URL from the ARC record header.</p>
   *
   * @return The URL for this entry.
   */
  public String getURL() {
    return url;
  }

  /**
   * <p>Returns the IP address from the ARC record header.</p>
   *
   * @return The IP address for this entry.
   */
  public String getIpAddress() {
    return ipAddress;
  }

  /**
   * <p>Returns the archive date from the ARC record header.</p>
   *
   * @return The archive date for this entry.
   */
  public Date getArchiveDate() {
    return archiveDate;
  }

  /**
   * <p>Returns the MIME content type from the ARC record header.</p>
   * <p>Note: The MIME content type in the ARC record header is not necessarily the
   * same as the <code>Content-Type</code> HTTP header inside the content body 
   * (if one is present).</p>
   *
   * @return The MIME content type for this entry.
   */
  public String getContentType() {
    return contentType;
  }

  /**
   * <p>Returns the content length from the ARC record header.</p>
   * <p>Note: The content length in the ARC record header is not necessarily the
   * same as the <code>Content-Length</code> HTTP header inside the content body 
   * (if one is present).</p>
   *
   * @return The content length for this entry.
   */
  public int getContentLength() {
    return contentLength;
  }

  /**
   * <p>Returns the HTTP status code.</p>
   * <p>If the payload could not be parsed as an HTTP response, returns -1.</p>
   * <p>Warning: if the payload has not yet been parsed as an HTTP response,
   * calling this function parses the full response.  Parsing is only performed
   * once - parsed data is retained for subsequent calls.</p>
   *
   * @return The HTTP status code.
   */
  public int getHttpStatusCode() throws IOException, HttpException {

    HttpResponse httpResponse = this.getHttpResponse();

    if (httpResponse == null) {
      return -1;
    }

    return httpResponse.getStatusLine().getStatusCode();
  }

  /**
   * <p>Returns an array of HTTP headers.</p>
   * <p>If the payload could not be parsed as an HTTP response, returns <code>null</code>.</p>
   * <p>Warning: if the payload has not yet been parsed as an HTTP response,
   * calling this function parses the full response.  Parsing is only performed
   * once - parsed data is retained for subsequent calls.</p>
   *
   * @return An array of HTTP headers.
   */
  public Header[] getHttpHeaders() throws IOException, HttpException {

    HttpResponse httpResponse = this.getHttpResponse();

    if (httpResponse == null) {
      return null;
    }

    return httpResponse.getAllHeaders();
  }

  public static class ByteArraySessionInputBuffer extends AbstractSessionInputBuffer {

    public ByteArraySessionInputBuffer(byte[] buf) {
      BasicHttpParams params = new BasicHttpParams();
      this.init(new ByteArrayInputStream(buf), 4096, params);
    }

    public ByteArraySessionInputBuffer(byte[] buf, int offset, int length) {
      BasicHttpParams params = new BasicHttpParams();
      this.init(new ByteArrayInputStream(buf, offset, length), 4096, params);
    }

    public boolean isDataAvailable(int timeout) {
      return true;
    }
  }

  /**
   * <p>Helper function to search a byte array for CR-LF-CR-LF (the end of
   * HTTP headers in the payload buffer).</p>
   *
   * @return The offset of the end of HTTP headers, after the last CRLF.
   */
  private int searchForCRLFCRLF(byte[] data) {

    final byte CR = (byte)'\r';
    final byte LF = (byte)'\n';

    int i;
    int s = 0;

    for (i = 0; i < data.length; i++) {

      if      (data[i] == CR) {
        if      (s == 0) s = 1;
        else if (s == 1) s = 0;
        else if (s == 2) s = 3;
        else if (s == 3) s = 0;
      }
      else if (data[i] == LF) {
        if      (s == 0) s = 0;
        else if (s == 1) s = 2;
        else if (s == 2) s = 0;
        else if (s == 3) s = 4;
      }
      else {
        s = 0;
      }

      if (s == 4)
        return i + 1;
    }

    return -1;
  }

  /**
   * <p>Returns an HTTP response object parsed from the ARC record payload.<p>
   * <p>Note: The payload is parsed on-demand, but is only parsed once.  The
   * parsed data is saved for subsequent calls.</p>
   *
   * @return The ARC record payload as an HTTP response object.  See the Apache
   * HttpComponents project.
   */
  public HttpResponse getHttpResponse() throws IOException, HttpException {

    if (httpResponse != null) {
      return httpResponse;
    }

    if (payload == null) {
      LOG.error("Unable to parse HTTP response: Payload has not been set");
      return null;
    }

    if (url != null && !url.startsWith("http://") && !url.startsWith("https://")) {
      LOG.error("Unable to parse HTTP response: URL protocol is not HTTP");
      return null;
    }

    httpResponse = null;

    // Find where the HTTP headers stop
    int end = searchForCRLFCRLF(payload);

    if (end == -1) {
      LOG.error("Unable to parse HTTP response: End of HTTP headers not found");
      return null;
    }

    // Parse the HTTP status line and headers
    DefaultHttpResponseParser parser =
      new DefaultHttpResponseParser(
        new ByteArraySessionInputBuffer(payload, 0, end),
        new BasicLineParser(),
        new DefaultHttpResponseFactory(),
        new BasicHttpParams()
      );

    httpResponse = parser.parse();

    if (httpResponse == null) {
      LOG.error("Unable to parse HTTP response");
      return null;
    }      

    // Set the reset of the payload as the HTTP entity.  Use an InputStreamEntity
    // to avoid a memory copy.
    InputStreamEntity entity = new InputStreamEntity(new ByteArrayInputStream(payload, end, payload.length - end),
        payload.length - end);
    entity.setContentType(httpResponse.getFirstHeader("Content-Type"));
    entity.setContentEncoding(httpResponse.getFirstHeader("Content-Encoding"));
    httpResponse.setEntity(entity);

    return httpResponse;
  }
}
