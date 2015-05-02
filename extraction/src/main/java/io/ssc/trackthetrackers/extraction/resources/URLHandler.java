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

  public static boolean couldBeUrl(String url) {

    if (!url.contains(".")) {
      return false;
    }

    //remove and check white space
    String trimmedUrl = url.trim();
    if (trimmedUrl.contains(" ") || trimmedUrl.contains("\t") || trimmedUrl.contains("\r") ||
        trimmedUrl.contains("\n")) {
      return false;
    }

    //TODO: check this condition
    //this doesnt work for something like localhost:80/...
    int colonIndex = trimmedUrl.indexOf(':');
    if (colonIndex != -1) {
      if (colonIndex < trimmedUrl.length() - 1 && trimmedUrl.charAt(colonIndex + 1) != '/') {
        return false;
      }
    }

    return true;
  }

  public static boolean isValidDomain(String url) {
    int startTopLevelDomain = url.lastIndexOf('.');
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

  public static String extractHost(String urlString) throws MalformedURLException {

    urlString = urlString.trim();

    if (urlString.isEmpty()) {           // permit empty
      return urlString;
    }

    //add protocol if not existent
    if (urlString.startsWith(".")) {
      urlString = urlString.substring(1);
    }

    if (!urlString.contains(":")) {
      if (!urlString.startsWith("//")) {
        urlString = "//" + urlString;
      }
      urlString = ":" + urlString;
    }

    if (urlString.startsWith(":")) {
      urlString = "http" + urlString;
    }

    return new URL(urlString).getHost().toLowerCase();
  }

}

