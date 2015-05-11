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
  public void testScriptLinkExtraction() {
    String html = "<script src=\"http://i.po.st/share/script/post-\n" +
        "widget.js#publisherKey=Inquisitr.com-607\" type=\"text/javascript\" charset=\"utf-\n" +
        "8\"></script>";
    Iterable<Resource> resources = extractor.extractResources("inquisitr.com", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("i.po.st", resource.url());
  }

  @Test
  public void testScriptLink2Extraction() {
    String html = "<script type=\"text/javascript\">\n" +
        "  var _gaq = _gaq || [];\n" +
        "  _gaq.push(['_setAccount', 'UA-152889-1']);\n" +
        "\t_gaq.push(['_setLocalGifPath','http://pixel.tradekey.com:8080/stats/counter.jsp']); </script>";
    Iterable<Resource> resources = extractor.extractResources("tradekey.com", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("pixel.tradekey.com", resource.url());
  }

  @Test
  public void testScriptLink3Extraction() {
    String html = "<script type=\"text/javascript\">\n" +
        "\n" +
        "\t\t\t\t  var _gaq = _gaq || [];\n" +
        "\t\t\t\t  _gaq.push(['_setAccount', 'UA-25996432-1']);\n" +
        "\t\t\t\t  _gaq.push(['_trackPageview']);\n" +
        "\n" +
        "\t\t\t\t  (function() {\n" +
        "\t\t\t\t    var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;\n" +
        "\t\t\t\t    ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '" +
        ".google-analytics.com/ga.js';\n" +
        "\t\t\t\t    var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);\n" +
        "\t\t\t\t  })();\n" +
        "\n" +
        "\t\t\t\t</script>";
    Iterable<Resource> resources = extractor.extractResources("travel.webshots.com", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("google-analytics.com", resource.url());
  }

  @Test
  public void testScriptLink4Extraction() {
    String html = "<script type=\"text/javascript\">document.write(\"&amp;domain=cruise-ships.ca\");</script>";
    Iterable<Resource> resources = extractor.extractResources("n49.ca", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("cruise-ships.ca", resource.url());
  }

  @Test
  public void testScriptLink5Extraction() {
    String html = "<script type=\"text/javascript\" src=\" http://w.sharethis.com/button/buttons.js\"></script>";
    Iterable<Resource> resources = extractor.extractResources("radaronline.com", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("w.sharethis.com", resource.url());
  }

  @Test
  public void testScriptLink6Extraction() {
    String html = "<script type=\"text/javascript\" src=\" http://pagead2.googlesyndication.com/pagead/show_ads" +
        ".js\"></script>";
    Iterable<Resource> resources = extractor.extractResources("springerlink.com", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("pagead2.googlesyndication.com", resource.url());
  }

  @Test
  public void testScriptLink7Extraction() {
    String html = "<script type=\"text/javascript\" src=\"http://eatps.web.aol" +
        ".com:9000/open_web_adhoc?subtype=7051&sid=LATINO&site=4\"></script>";
    Iterable<Resource> resources = extractor.extractResources("deportes.aollatino.com", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("eatps.web.aol.com", resource.url());
  }

  @Test
  public void testScriptLink8Extraction() {
    String html = "<SCRIPT language=javascript>\n" +
        "function ztc(){\n" +
        "var objztc=document.getElementById(\"search\");\n" +
        "var keyWord=objztc.word.value;\n" +
        "var url=\"http://adagent.21cn.com:8082/user/user_pages_straight.jsp?KeywordName=\"+keyWord;\n" +
        "window.open(url,'','');\n" +
        "}\n" +
        "</SCRIPT>";
    Iterable<Resource> resources = extractor.extractResources("travel.21cn.com", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.SCRIPT, resource.type());
    assertEquals("adagent.21cn.com", resource.url());
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
  public void testIFRAMELinkExtraction() {
    String html = "<iframe src=\"http://ad.doubleclick.net/adi/agi.ws.browse/page/photo/homegarden/hobbiesinterests;" +
        "tile=1;tag=_TAG_;search=dogs\n" +
        ";a=_WSAGE_;g=;dcopt=ist;sz=300x250;ord=30819?\" width=\"300\" height=\"250\" marginwidth=\"0\" " +
        "marginheight=\"0\" hspace=\"0\" vspace=\"0\" frameborder=\"0\" scrolling=no borderCOLOR=\"#000000\">\n" +
        "<script language=\"JavaScript\" src=\"http://ad.doubleclick.net/adj/agi.ws" +
        ".browse/page/photo/homegarden/hobbiesinterests;tile=1;tag=_TAG_;search=dogs\n" +
        ";a=_WSAGE_;g=;dcopt=ist;sz=300x250;abr=!ie;ord=30819?\" type=\"text/javascript\"></script>\n" +
        "<noscript><a href=\"http://ad.doubleclick.net/jump/agi.ws.browse/page/photo/homegarden/hobbiesinterests;" +
        "tile=1;tag=_TAG_;search=dogs\n" +
        ";a=_WSAGE_;g=;sz=300x250;abr=!ie4;abr=!ie5;abr=!ie6;ord=30819?\" target=\"_blank\"><img src=\"http://ad" +
        ".doubleclick.net/ad/agi.ws.browse/page/photo/homegarden/hobbiesinterests;tile=1;tag=_TAG_;search=dogs\n" +
        ";a=_WSAGE_;g=;sz=300x250;abr=!ie4;abr=!ie5;abr=!ie6;ord=30819?\" width=\"300\" height=\"250\" border=\"0\" " +
        "alt=\"\"></a></noscript>\n" +
        "</iframe>";
    Iterable<Resource> resources = extractor.extractResources("home-and-garden.webshots.com", html);

    Resource resource = Iterables.getOnlyElement(resources);
    assertEquals(Resource.Type.IFRAME, resource.type());
    assertEquals("ad.doubleclick.net", resource.url());
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
  public void testURLValidation() {
    String html = "<iframe src=\"http://google+wrong+domain.com/\"></iframe>" +
        "<iframe src=\"http://user@google.com/\"></iframe>" +
        "<iframe src=\"http://user(at)google.com/\"></iframe>";
    Iterable<Resource> resources = extractor.extractResources("web.de", html);
    assertEquals(resources.iterator().hasNext(), false);
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
