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

import static org.junit.Assert.assertTrue;

public class PhantomJSvsGoogleTest {

  @Test
  public void spiegelDe() throws IOException {

    Iterable<Resource> resourcesGoogle = extractResources("http://spiegel.de", Resources.getResource("spiegel.de.html"));
      
    Iterable<Resource> resourcesPhantom = extractPhantomJSResources("http://spiegel.de", Resources.getResource("spiegel.de.html"));

    compareResults(resourcesGoogle, resourcesPhantom);
  }

  @Test
  public void zalandoDe() throws IOException {

    Iterable<Resource> resourcesGoogle = extractResources("http://zalando.de", Resources.getResource("zalando.de.html"));

    Iterable<Resource> resourcesPhantom = extractPhantomJSResources("http://zalando.de", Resources.getResource("zalando.de.html"));

    compareResults(resourcesGoogle, resourcesPhantom);
  }

  @Test
  public void rtlDe() throws IOException {
    Iterable<Resource> resourcesGoogle = extractResources("http://rtl.de", Resources.getResource("rtl.de.html"));

    Iterable<Resource> resourcesPhantom = extractPhantomJSResources("http://rtl.de", Resources.getResource("rtl.de.html"));

    compareResults(resourcesGoogle, resourcesPhantom);
  }

  @Test
  public void mediamarktDe() throws IOException {
    Iterable<Resource> resourcesGoogle = extractResources("http://www.mediamarkt.de", Resources.getResource("mediamarkt.de.html"));

    Iterable<Resource> resourcesPhantom = extractPhantomJSResources("http://www.mediamarkt.de", Resources.getResource("mediamarkt.de.html"));

    compareResults(resourcesGoogle, resourcesPhantom);
  }

  @Test
  public void techcrunchCom() throws IOException {

    Iterable<Resource> resourcesGoogle = extractResources("http://techcrunch.com", Resources.getResource("techcrunch.com.html"));

    Iterable<Resource> resourcesPhantom = extractPhantomJSResources("http://techcrunch.com", Resources.getResource("techcrunch.com.html"));

    compareResults(resourcesGoogle, resourcesPhantom); 
  }

  @Test
  public void theguardianCom() throws IOException {

    Iterable<Resource> resourcesGoogle = extractResources("http://theguardian.com", Resources.getResource("theguardian.com.html"));

    Iterable<Resource> resourcesPhantom = extractPhantomJSResources("http://theguardian.com", Resources.getResource("theguardian.com.html"));

    compareResults(resourcesGoogle, resourcesPhantom);
  }

  @Test
  public void buzzfeedCom() throws IOException {

    Iterable<Resource> resourcesGoogle = extractResources("http://buzzfeed.com", Resources.getResource("buzzfeed.com.html"));

    Iterable<Resource> resourcesPhantom = extractPhantomJSResources("http://buzzfeed.com", Resources.getResource("buzzfeed.com.html"));

    compareResults(resourcesGoogle, resourcesPhantom);
  }


  @Test
  public void prosiebenDe() throws IOException {

    Iterable<Resource> resourcesGoogle = extractResources("http://prosieben.de", Resources.getResource("prosieben.de.html"));

    Iterable<Resource> resourcesPhantom = extractPhantomJSResources("http://prosieben.de", Resources.getResource("prosieben.de.html"));

    compareResults(resourcesGoogle, resourcesPhantom);
  }



  private void compareResults(Iterable<Resource> googleResources, Iterable<Resource> phantomJSResources) {

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
      if(viewersExtractedPhantomJS.contains(googleUrl)) {
        matches.add(googleUrl);
      } else {
        onlyGoogle.add(googleUrl);
      }
    }
    for (String phantomJSUrl : viewersExtractedPhantomJS) {
      if(!viewersExtractedGoogle.contains(phantomJSUrl)) {
        onlyPhantomJS.add(phantomJSUrl);
      }
    }

    System.out.println("--- FOUND BY BOTH ----");
    for (String url : matches) {
      System.out.println("\t" + url);
    }
    System.out.println("--- ONLY FOUND BY GOOGLE JS PARSER ----");
    for (String url : onlyGoogle) {
      System.out.println("\t" + url);
    }
    System.out.println("--- ONLY FOUND BY PHANTOMJS ----");
    for (String url : onlyPhantomJS) {
      System.out.println("\t" + url);
    }
  }
    
  String normalize(String url) {
    String [] parts = url.split("\\.");

    if (parts.length > 2) {
      String normUrl = parts[parts.length - 2] + "." + parts[parts.length - 1];
      return normUrl;
    }
    return url;
  }   

  Iterable<Resource> extractResources(String sourceUrl, URL page) throws IOException {
    return new ResourceExtractor().extractResources(sourceUrl, Resources.toString(page, Charsets.UTF_8));
  }

  Iterable<Resource> extractPhantomJSResources(String sourceUrl, URL page) throws IOException {
    GhostDriverExtractor gex = new GhostDriverExtractor();
    return gex.extractResources(sourceUrl, Resources.toString(page, Charsets.UTF_8));
  }

}
