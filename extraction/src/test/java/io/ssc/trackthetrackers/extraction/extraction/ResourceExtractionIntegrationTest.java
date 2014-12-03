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
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Set;
import java.util.SortedSet;

import static org.junit.Assert.assertTrue;

public class ResourceExtractionIntegrationTest {

  @Test
  public void spiegelDe() throws IOException {

    Iterable<Resource> resources = extractResources("http://spiegel.de", Resources.getResource("spiegel.de.html"));

    assertViewersFound(resources, "spiegel.ivwbox.de", "adserv.quality-channel.de", "www.facebook.com",
                                  "platform.twitter.com");
  }

  @Test
  public void zalandoDe() throws IOException {

    Iterable<Resource> resources = extractResources("http://zalando.de", Resources.getResource("zalando.de.html"));

    assertViewersFound(resources, "www.everestjs.net", "pixel.everesttech.net", "ad-emea.doubleclick.net",
                                  "fls.doubleclick.net", "uidbox.uimserv.net", "www.googleadservices.com",
                                  "google-analytics.com", "www.facebook.com", "connect.facebook.net",
                                  "sonar.sociomantic.com", "skin.ztat.net");
  }

  @Test
  public void rtlDe() throws IOException {
    Iterable<Resource> resources =
        extractResources("http://rtl.de", Resources.getResource("rtl.de.html"));

    assertViewersFound(resources, "bilder.akamai.rtl.de", "script.ioam.de", "google-analytics.com", "ip.nuggad.net",
                                  "pq-direct.revsci.net", "autoimg.rtl.de", "count.rtl.de", "ad.de.doubleclick.net",
                                  "connect.facebook.net", "autoimg.static-fra.de", "autoimg.clipfish.de",
                                  "pagead2.googlesyndication.com");
  }

  @Test
  public void mediamarktDe() throws IOException {
    Iterable<Resource> resources =
        extractResources("http://www.mediamarkt.de", Resources.getResource("mediamarkt.de.html"));

    assertViewersFound(resources, "www.etracker.de", "css.redblue.de", "js.redblue.de", "data.mediamarkt.de",
                                  "ad.doubleclick.net", "code.etracker.com");
  }

  @Test
  public void techcrunchCom() throws IOException {

    Iterable<Resource> resources =
        extractResources("http://techcrunch.com", Resources.getResource("techcrunch.com.html"));

    assertViewersFound(resources, "pshared.5min.com", "o.aolcdn.com", "static.chartbeat.com", "connect.facebook.net",
                                  "js.adsonar.com", "s.gravatar.com", "stats.wordpress.com", "google-analytics.com",
                                  "cdn.insights.gravity.com", "d.adsbyisocket.com", "quantserve.com",
                                  "scorecardresearch.com", "platform.twitter.com", "disqus.com");
  }

  @Test
  public void theguardianCom() throws IOException {

    Iterable<Resource> resources =
        extractResources("http://theguardian.com", Resources.getResource("theguardian.com.html"));

    assertViewersFound(resources, "static.guim.co.uk", "combo.guim.co.uk", "ajax.googleapis.com", "pasteup.guim.co.uk",
                                  "www.googletagservices.com", "resource.guim.co.uk", "dqwufkbc3sdtr.cloudfront.net",
                                  "assets.guim.co.uk", "id.guim.co.uk", "gu-text-catcher.appspot.com",
                                  "cdn.optimizely.com", "ajax.googleapis.com", "j.ophan.co.uk", "hits.theguardian.com",
                                  "req.connect.wunderloop.net", "pq-direct.revsci.net", "widgets.outbrain.com",
                                  "secure-uk.imrworldwide.com", "a248.e.akamai.net", "static.chartbeat.com",
                                  "cdn.krxd.net", "www.googleadservices.com", "googleads.g.doubleclick.net",
                                  "www.google.com");
  }

  @Test
  public void buzzfeedCom() throws IOException {

    Iterable<Resource> resources =
        extractResources("http://buzzfeed.com", Resources.getResource("buzzfeed.com.html"));

    assertViewersFound(resources, "s3-ak.buzzfed.com", "www.googletagservices.com", "ct.buzzfeed.com",
                                  "google-analytics.com", "stats.g.doubleclick.net", "quantserve.com", "scorecardresearch.com",
                                  "ad.doubleclick.net", "pixel.quantserve.com",
                                  "www.facebook.com", "ads.audienceamplify.com", "rtd.tubemogul.com", "ib.adnxs.com",
                                  "connect.facebook.net");
  }


  private void assertViewersFound(Iterable<Resource> resources, String... urls) {

    Set<String> viewersExtracted = Sets.newHashSet();
    for (Resource resource : resources) {
      viewersExtracted.add(resource.url());
    }

    SortedSet<String> viewersFound = Sets.newTreeSet();
    SortedSet<String> viewersMissed = Sets.newTreeSet();

    for (String url : urls) {
      if (viewersExtracted.contains(url)) {
        viewersFound.add(url);
      } else {
        viewersMissed.add(url);
      }
    }

    SortedSet<String> viewersFoundAdditionally = Sets.newTreeSet(viewersExtracted);
    viewersFoundAdditionally.removeAll(Sets.newHashSet(urls));

    System.out.println("--- FOUND ----");
    for (String url : viewersFound) {
      System.out.println("\t" + url);
    }
    System.out.println("--- MISSED ----");
    for (String url : viewersMissed) {
      System.out.println("\t" + url);
    }
    System.out.println("--- ADDITIONALLY ----");
    for (String url : viewersFoundAdditionally) {
      System.out.println("\t" + url);
    }


    assertTrue(viewersMissed.isEmpty());
  }

  Iterable<Resource> extractResources(String sourceUrl, URL page) throws IOException {
    return new ResourceExtractor().extractResources(sourceUrl, Resources.toString(page, Charsets.UTF_8));
  }

}
