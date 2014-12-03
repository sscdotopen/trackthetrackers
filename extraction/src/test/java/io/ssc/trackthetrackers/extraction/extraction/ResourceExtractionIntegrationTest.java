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
import com.google.common.io.Resources;
import io.ssc.trackthetrackers.extraction.resources.Resource;
import io.ssc.trackthetrackers.extraction.resources.ResourceExtractor;
import org.junit.Test;
import org.junit.Assert;

import java.io.IOException;
import java.net.URL;

public class ResourceExtractionIntegrationTest {

  @Test
  public void spiegelDe() throws IOException {

    Iterable<Resource> resources = extractResources("http://spiegel.de", Resources.getResource("spiegel.de.html"));

    for (Resource resource : resources) {
      System.out.println(resource);
    }

    assertViewersFound(resources, "spiegel.ivwbox.de", "adserv.quality-channel.de", "www.facebook.com", "platform.twitter.com");
  }

  @Test
  public void zalandoDe() throws IOException {

    Iterable<Resource> resources = extractResources("http://zalando.de", Resources.getResource("zalando.de.html"));

    for (Resource resource : resources) {
      System.out.println(resource);
    }

      assertViewersFound(resources, "www.everestjs.net", "pixel.everesttech.net", "ad-emea.doubleclick.net", "fls.doubleclick.net", "uidbox.uimserv.net",
       "www.googleadservices.com", "google-analytics.com", "www.facebook.com", "connect.facebook.net", "sonar.sociomantic.com", "skin.ztat.net");
  }

  @Test
  public void techcrunchCom() throws IOException {

    Iterable<Resource> resources = extractResources("http://techcrunch.com",
        Resources.getResource("techcrunch.com.html"));

    for (Resource resource : resources) {
      System.out.println(resource);
    }

    assertViewersFound(resources, "pshared.5min.com", "o.aolcdn.com", "static.chartbeat.com", "connect.facebook.net","js.adsonar.com", "s.gravatar.com", "stats.wordpress.com",
            "google-analytics.com", "cdn.insights.gravity.com","d.adsbyisocket.com", "quantserve.com", "scorecardresearch.com", "platform.twitter.com");

    //the following were missed:
    //  assertViewersFound(resources,"disqus.com");
  }


  private void assertViewersFound(Iterable<Resource> resources, String... urls) {
    for (String url : urls) {
      boolean found = false;
      for (Resource resource : resources) {
        if (resource.url().contains(url)) {
          found = true;
          System.out.println("Found resource " + url);
          break;
        }
      }
      Assert.assertTrue("Resource extraction missed [" + url + "]",found);
    }
  }

  Iterable<Resource> extractResources(String sourceUrl, URL page) throws IOException {
    return new ResourceExtractor().extractResources(sourceUrl, Resources.toString(page, Charsets.UTF_8));
  }

}
