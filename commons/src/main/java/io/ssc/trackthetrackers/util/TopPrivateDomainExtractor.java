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

import com.google.common.net.InternetDomainName;

import java.net.URI;
import java.net.URISyntaxException;

public class TopPrivateDomainExtractor {

  private TopPrivateDomainExtractor() {}

  public static String extract(String url) throws URISyntaxException {

    String urlToInspect = url.replaceAll("\\[", "").replaceAll("\\]", "")
                             .replaceAll("\\{", "").replaceAll("}", "")
                             .replaceAll("_", "")
                             .replaceAll("<", "").replaceAll(">", "")
                             .replaceAll("\\|", "").replaceAll("\\^", "").replaceAll("\\\\", "").replaceAll("%", "");

    String domainToInspect = urlToInspect;

    if (domainToInspect.startsWith("http://")) {
      domainToInspect = new URI(urlToInspect).getHost();

      domainToInspect.replaceAll("http://", "");

      if (domainToInspect.endsWith("/")) {
        domainToInspect = domainToInspect.substring(0, domainToInspect.length() - 1);
      }
    }

    if (domainToInspect.endsWith(".amazonaws.com")) {
      return "amazonaws.com";
    }

    if (domainToInspect.endsWith(".cloudfront.net") || domainToInspect.equalsIgnoreCase("cloudfront.net")) {
      return "cloudfront.net";
    }

    if (domainToInspect.endsWith(".googlecode.com")) {
      return "googlecode.com";
    }

    if (domainToInspect.endsWith(".blogspot.com") || domainToInspect.equalsIgnoreCase("blogspot.com")) {
      return "blogspot.com";
    }

    if (domainToInspect.endsWith(".googleapis.com")) {
      return "googleapis.com";
    }

    if (domainToInspect.endsWith(".appspot.com")) {
      return "appspot.com";
    }


    return InternetDomainName.from(domainToInspect).topPrivateDomain().toString();
  }
}
