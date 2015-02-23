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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import io.ssc.trackthetrackers.extraction.resources.Resource;
import io.ssc.trackthetrackers.extraction.resources.ResourceExtractor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ResourceExtractionTest {

  private ResourceExtractor extractor;

  @Before
  public void initializeExtractor() {
    extractor = new ResourceExtractor();
  }

  @Test
  public void testDomainLinkExtraction() {

    String html = "</script><link rel=\"stylesheet\" type=\"text/css\" href=\"ftp://media.ztat.net\"/><script type=\"text/javascript\">";
    Iterable<Resource> resources = extractor.extractResources("zalando.de", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.LINK, resource.type());
    assertEquals("media.ztat.net", resource.url());
  }

  @Test
  public void testLinkExtraction() {
    String html = "</script><link rel=\"stylesheet\" type=\"text/css\" href=\"http://media.ztat.net/media/cms/css/cms/fix/addition-landing-block.css?_=1340992059000\"/><script type=\"text/javascript\">";
    Iterable<Resource> resources = extractor.extractResources("zalando.de", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.LINK, resource.type());
    assertEquals("media.ztat.net", resource.url());
  }

  @Test
  public void testImageExtraction() {
    String html = "<a name=\"header.logo\" href=\"http://www.zalando.de/index.html\"><img height=\"53\" width=\"190\" src=\"http://skin.ztat.net/s/04g/img/logos/shops/schuhe.jpg\" alt=\"Schuhe und Mode bei Zalando.de\" title=\"Schuhe und Mode bei Zalando.de\"/></a>";
    Iterable<Resource> resources = extractor.extractResources("zalando.de", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.IMAGE, resource.type());
    assertEquals("skin.ztat.net", resource.url());
  }

  @Test
  public void testIframeExtraction() {
    String html = "<iframe class=\"left\" src=\"http://ad-emea.doubleclick.net/adi/z.de.hp/hp_bottom_left;tile=5;sz=500x310;ord=1341835861857\" width=\"500\" height=\"310\" marginwidth=\"0\" marginheight=\"0\" frameborder=\"0\" scrolling=\"no\"></iframe>";
    Iterable<Resource> resources = extractor.extractResources("zalando.de", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.IFRAME, resource.type());
    assertEquals("ad-emea.doubleclick.net", resource.url());
  }

  @Test
  public void testScriptExtraction () {
    String html = "<script src=\"http://skin.ztat.net/s/04g/js/zalando.min.js\"  type=\"text/javascript\"></script>";
    Iterable<Resource> resources = extractor.extractResources("zalando.de", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("skin.ztat.net", resource.url());
  }

  @Test
  public void testJavascriptExtraction() {
    String html = "<script type=\"text/javascript\"> (function() { var s = document.createElement(\"script\"); s.id = \"sonar-adpan\"; s.src = (\"https:\" == document.location.protocol ? \"https://\" : \"http://\") + \"sonar.sociomantic.com/js/2010-07-01/adpan/zalando-de\";</script>";
    Iterable<Resource> resources = extractor.extractResources("zalando.de", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("sonar.sociomantic.com", resource.url());

    html = "<script type=\"text/javascript\"> (function() { var s = document.createElement(\"script\"); s.id = \"sonar-adpan\"; s.src = (\"https:\" == document.location.protocol ? \"https://\" : \"http://\")+\"you.target.de/js/2010-07-01/adpan/zalando-de\";</script>";
    resources = extractor.extractResources("zalando.de", html);

    resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("you.target.de", resource.url());

    html = "<script type=\"text/javascript\"> (function() { var s = document.createElement(\"script\"); s.id = \"sonar-adpan\"; s.src = (\"https:\" == document.location.protocol ? \"https://\" : \"ftp://\")+\"you.target.de/js/2010-07-01/adpan/zalando-de\";</script>";
    resources = extractor.extractResources("zalando.de", html);

    resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("you.target.de", resource.url());

    html = "<script type=\"text/javascript\"> (function() { var s = document.createElement(\"script\"); s.id = \"sonar-adpan\"; s.src = (\"https:\" == document.location.protocol ? \"https://\" : \"https://\")+\"you.target.de/js/2010-07-01/adpan/zalando-de\";</script>";
    resources = extractor.extractResources("zalando.de", html);

    resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("you.target.de", resource.url());
  }

  @Test
  public void testSeveralExtractions () {
    String html = "</script><link rel=\"stylesheet\" type=\"text/css\" href=\"ftp://media.ztat.net\"/><script type=\"text/javascript\">";
    html += "</script><link rel=\"stylesheet\" type=\"text/css\" href=\"http://media.ztat.net/media/cms/css/cms/fix/addition-landing-block.css?_=1340992059000\"/>";
    html += "<a name=\"header.logo\" href=\"http://www.zalando.de/index.html\"><img height=\"53\" width=\"190\" src=\"http://skin.ztat.net/s/04g/img/logos/shops/schuhe.jpg\" alt=\"Schuhe und Mode bei Zalando.de\" title=\"Schuhe und Mode bei Zalando.de\"/></a>";
    html += "<iframe class=\"left\" src=\"http://ad-emea.doubleclick.net/adi/z.de.hp/hp_bottom_left;tile=5;sz=500x310;ord=1341835861857\" width=\"500\" height=\"310\" marginwidth=\"0\" marginheight=\"0\" frameborder=\"0\" scrolling=\"no\"></iframe>";
    html += "<script src=\"http://skin.ztat.net/s/04g/js/zalando.min.js\"  type=\"text/javascript\"></script>";

    Iterable<Resource> resources = extractor.extractResources("zalando.de", html);

    for (Resource resource : resources) {
      System.out.println(resource);
    }

    assertViewersFound(resources, "ad-emea.doubleclick.net", "media.ztat.net", "skin.ztat.net");
    assertEquals(4, Iterables.size(resources));

  }

  @Test
  public void ignoreJavascript () {
    String html = "<script src=\"/layout/js/http/netmind-V3-13-3.js\" type=\"text/javascript\"></script>";
    Iterable<Resource> resources = extractor.extractResources("spiegel.de", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    System.out.println("result: " + resource.url());
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
      Assert.assertTrue("Resource extraction missed [" + url + "]", found);
    }
  }

}
