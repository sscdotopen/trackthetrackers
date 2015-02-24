/**
 * Track the trackers
 * Copyright (C) 2014  Sebastian Schelter, Felix Neutatz
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

package io.ssc.trackthetrackers.extraction.extraction;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import io.ssc.trackthetrackers.extraction.resources.Resource;
import io.ssc.trackthetrackers.extraction.resources.ResourceExtractor;
import io.ssc.trackthetrackers.extraction.resources.GhostDriverExtractor;

import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Set;
import java.util.SortedSet;

public class PhantomJSvsGoogleTest {

  @Test
  public void spiegelDe() throws IOException {
    evaluate("http://spiegel.de", "spiegel.de.html");
  }

  @Test
  public void zalandoDe() throws IOException {
    evaluate("http://zalando.de", "zalando.de.html");
  }

  @Test
  public void rtlDe() throws IOException {
    evaluate("http://rtl.de", "rtl.de.html");
  }

  @Test
  public void mediamarktDe() throws IOException {
    evaluate("http://www.mediamarkt.de", "mediamarkt.de.html");
  }

  @Test
  public void techcrunchCom() throws IOException {
    evaluate("http://techcrunch.com", "techcrunch.com.html");
  }

  @Test
  public void theguardianCom() throws IOException {
    evaluate("http://theguardian.com", "theguardian.com.html");
  }

  @Test
  public void buzzfeedCom() throws IOException {
    evaluate("http://buzzfeed.com", "buzzfeed.com.html");
  }

  @Test
  public void prosiebenDe() throws IOException {
    evaluate("http://prosieben.de", "prosieben.de.html");
  }


  private void evaluate(String uri, String htmlFile) throws IOException {

    Iterable<Resource> googleResources = extractResources(uri, Resources.getResource(htmlFile));
    Iterable<Resource> phantomJSResources = extractPhantomJSResources(uri, Resources.getResource(htmlFile));


    Set<String> viewersExtractedGoogle = Sets.newHashSet();
    for (Resource resource : googleResources) {
      viewersExtractedGoogle.add(normalize(resource.url()));
    }

    Set<String> viewersExtractedPhantomJS = Sets.newHashSet();
    for (Resource resource : phantomJSResources) {
      viewersExtractedPhantomJS.add(normalize(resource.url()));
    }

    SortedSet<String> matches = Sets.newTreeSet();
    SortedSet<String> onlyGoogle = Sets.newTreeSet();
    SortedSet<String> onlyPhantomJS = Sets.newTreeSet();

    for (String googleUrl : viewersExtractedGoogle) {
      if (viewersExtractedPhantomJS.contains(googleUrl)) {
        matches.add(googleUrl);
      } else {
        onlyGoogle.add(googleUrl);
      }
    }

    for (String phantomJSUrl : viewersExtractedPhantomJS) {
      if (!viewersExtractedGoogle.contains(phantomJSUrl)) {
        onlyPhantomJS.add(phantomJSUrl);
      }
    }

    System.out.println("--- FOUND BY BOTH ----");
    for (String url : matches) {
      System.out.println("\t" + url);
    }

    System.out.println("--- ONLY FOUND BY PHANTOMJS ----");
    for (String url : onlyPhantomJS) {
      System.out.println("\t" + url);
    }

    System.out.println("--- ONLY FOUND BY GOOGLE JS PARSER (POTENTIAL FALSE POSITIVES) ----");
    for (String url : onlyGoogle) {
      System.out.println("\t" + url);
    }
  }
    
  String normalize(String url) {
    String [] parts = url.split("\\.");

    if (parts.length > 2) {
      return parts[parts.length - 2] + "." + parts[parts.length - 1];
    }
    return url;
  }   

  Iterable<Resource> extractResources(String sourceUrl, URL page) throws IOException {
    return new ResourceExtractor().extractResources(sourceUrl, Resources.toString(page, Charsets.UTF_8));
  }

  Iterable<Resource> extractPhantomJSResources(String sourceUrl, URL page) throws IOException {
    return new GhostDriverExtractor().extractResources(sourceUrl, Resources.toString(page, Charsets.UTF_8));
  }

}
