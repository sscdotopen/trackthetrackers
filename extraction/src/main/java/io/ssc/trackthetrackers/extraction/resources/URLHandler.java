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

import java.net.MalformedURLException;

class URLHandler {

  private URLHandler() {}

  public static boolean couldBeUrl(String url) {
    if (!url.contains(".") || url.contains(" ") || url.contains("\t") || url.contains("\r") || url.contains("\n") || url.contains("@")) {
      return false;
    }

    return true;
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

  public static String parseDomain(String url) {
    
    //parse domain from URL manually
    String nUrl = "";
    final int len = url.length();
    boolean isDot = false;
    int startDomain = 0;
    for (int i = 0; i < len; i++) {
      if (url.charAt(i) == '=') { //for javascript parsing src "=" domain -> to get a starting point
        if (startDomain <= i) {
          startDomain = i+1;
        }
      }
      
      if(isDot) {
        if (url.charAt(i) == '/') { // find google.com "/" to get the end point of domain
          nUrl = url.substring(startDomain,i);
          return nUrl;        
        }
        if (url.charAt(i) == ':') {  // find google.com ":" 8080  to get the end point of domain
          nUrl = url.substring(startDomain,i);
          return nUrl;
        }
      } else {
        if (url.charAt(i) == '/') { //find http: "//" to get a starting point
          startDomain = i + 1;
        }
        if (url.charAt(i) == '.') { //if the dots are present, we are in the domain part of the string
          isDot = true;
        }        
      }
    }
    return url.substring(startDomain);
  }

  public static String extractHost(String candidateUrl) throws MalformedURLException {

    if (candidateUrl.isEmpty()) {           // permit empty
      return candidateUrl;
    }

    String domain = parseDomain(candidateUrl);
    
    if (domain.startsWith(".")) {
      domain = domain.substring(1);
    }

    if (!DomainValidator.getInstance().isValid(domain)) {
      throw new MalformedURLException();
    }

    return domain;
  }

}

