/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz, Karim Wadie
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

package io.ssc.trackthetrackers.util;

import org.junit.Test;

import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class TopPrivateDomainExtractorTest {

  @Test
  public void problematicSamples() throws URISyntaxException {
    testExtraction("s3.amazonaws.com", "amazonaws.com");
    testExtraction("sealserver.trustkeeper.net", "trustkeeper.net");
    testExtraction("http://edwardmichaelgeorge.blogspot.com/", "blogspot.com");

    testExtraction("blogspot.com", "blogspot.com");
    testExtraction("s3-eu-west-1.amazonaws.com", "amazonaws.com");
    testExtraction("http://www.hrpraktijk.nl/kennisbank-gezondheidsbeleid/bewerkbare-modellen/arbeidsongeschiktheid/model-functionele-mogelijkheden-voorwaarden-[.524137.lynkx", "hrpraktijk.nl");
    testExtraction("http://www.slovreme.net/video_youtube/Avatar%20Movie%20Trailer%20[HD]/d1_JBMrrYw8", "slovreme.net");
    testExtraction("http://www.grbj.com/GRBJ/Nav/Login.htm?ArticleID={3DD014B4-F807-4130-9CC0-D29ABCDF35F2}", "grbj.com");
    testExtraction("http://packet_loss.blueprograms.com/", "blueprograms.com");
    testExtraction("cloudfront.net", "cloudfront.net");
    testExtraction("httpoolro.nuggad.net", "nuggad.net");
    testExtraction("http://www.adriatica.net/err/msg.htm?ERR=CompileHtml2Java>%20missing%20file:%20/guide/croatia/feature/plitvice.htm,%20language:%20SLO", "adriatica.net");
    testExtraction("http://address4sex.com/?view=not_found&id=811838&lang=en&reason=%20%20-%20This%20Ad%20Is%20Inactive%20or%20Expired%20<br%20/>", "address4sex.com");
    testExtraction("http://www.upromise.com/acquisitionLanding.do?aqid=yfamprnt&ax=cj|3015751&AID=10487593&PID=3015751", "upromise.com");
    testExtraction("http://www.globaljet.com.au/index.html?alerts=Page+not+found+%2Farticles%2Fbumpy-ride.html^^", "globaljet.com.au");
    testExtraction("http://www.birthdayinabox.com/party-themes/productdetail.asp?BTID\\=\\&tab_cat_id=8&prodsku=15464&subcat=205", "birthdayinabox.com");
    testExtraction("http://sinohome.org/bbs/viewthread.php?tid=11143&extra=page%", "sinohome.org");
    testExtraction("http://forum.hardware.fr/forum1.php?config=hfr.inc&cat=1&post_cat_list=|1*hfr|15*hfr|2*hfr|14*hfr|5*hfr|4*hfr|12*hfr|6*hfr|8*hfr|13*hfr|&trash=0&orderSearch=0&recherches=1&resSearch=200&jour=0&mois=0&annee=0&titre=3&search=&pseud=gandilflegras&daterange=2&searchtype=1&searchall=1",
                   "hardware.fr");
    testExtraction("http://woodstock-tell-a-vision.com/2008/08/14/richard-prans-juan-on-making-your-own-fuel/%&(%7B$%7Beval(base64_decode($_SERVER[HTTP_EXECCODE]))%7D%7D%7C.+)&%/feed/", "woodstock-tell-a-vision.com");
  }

  private void testExtraction(String url, String domain) throws URISyntaxException {
    assertEquals(domain, TopPrivateDomainExtractor.extract(url));
  }
}
