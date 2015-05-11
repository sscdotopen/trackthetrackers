/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz
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

package io.ssc.trackthetrackers.extraction.resources;

import org.apache.commons.validator.routines.DomainValidator;

import java.net.URL;
import java.net.MalformedURLException;

class URLHandler {

  private URLHandler() {}

  public static boolean couldBeUrl(String url) {
    if (!url.contains(".") || url.contains(" ") || url.contains("\t") || url.contains("\r") || url.contains("\n") || url.contains("@")) {
      return false;
    }
    
    //TODO: check this condition
    //this doesnt work for something like localhost:80/...
    int colonIndex = url.indexOf(':');
    if (colonIndex != -1) {
      if (colonIndex < url.length() - 1 && url.charAt(colonIndex + 1) != '/') {
        return false;
      }
    }

    return true;
  }

  public static boolean isValidDomain(String url) {
    int startTopLevelDomain = url.lastIndexOf('.');

    if (startTopLevelDomain <= 0) {
      return false;
    }

    String topLevelDomain = url.substring(startTopLevelDomain + 1);

    return DomainValidator.getInstance().isValidTld(topLevelDomain);
  }

  public static String expandIfInternalLink(String prefixForInternalLinks, String link) {
    if (!link.startsWith("http") && !link.startsWith("ftp") && !link.startsWith("https")) {
      if (!link.startsWith("//")) {
        return prefixForInternalLinks + link;
      } else {
        return "http:" + link;
      }
    }
    return link;
  }

  public static String createPrefixForInternalLinks(String sourceUrl) {
    String prefixForInternalLinks = sourceUrl;

    int lastSlashIndex = sourceUrl.lastIndexOf('/');
    if (lastSlashIndex == -1) {
      prefixForInternalLinks += '/';
    } else if (lastSlashIndex != prefixForInternalLinks.length() - 1) {
      prefixForInternalLinks = prefixForInternalLinks.substring(0, lastSlashIndex + 1);
    }
    return prefixForInternalLinks;
  }

  public static String cleanURL(String url) {
    String newUrl = url.split("\\?")[0].trim(); //remove php parameters
    newUrl = newUrl.split("#")[0]; //remove other stuff

    if (newUrl.startsWith("rtmp:")) {
      newUrl = newUrl.replace("rtmp:", "http:");
    }
    return newUrl;
  }

  public static String clearDomain(String domain) throws MalformedURLException {
    
    domain = domain.replace("\\.", ".");
       
    String splits[] = domain.split("=");
    
    if (domain.contains("(") || domain.contains(")") || domain.contains("+") || domain.contains("\\")) {
      throw new MalformedURLException();
    }
    
    return splits[splits.length - 1];
  }


  public static String extractHost(String candidateUrl) throws MalformedURLException {

    if (candidateUrl.isEmpty()) {           // permit empty
      return candidateUrl;
    }

    String url = cleanURL(candidateUrl);

    //add protocol if not existent
    if (url.startsWith(".")) {
      url = url.substring(1);
    }

    if (!url.contains(":")) {
      if (!url.startsWith("//")) {
        url = "//" + url;
      }
      url = ":" + url;
    }

    if (url.startsWith(":")) {
      url = "http" + url;
    }

    return clearDomain(new URL(url).getHost().toLowerCase());
  }

}

