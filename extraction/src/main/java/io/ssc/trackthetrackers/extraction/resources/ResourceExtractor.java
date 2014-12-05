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

package io.ssc.trackthetrackers.extraction.resources;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.validator.routines.DomainValidator;

import com.google.common.collect.Sets;

public class ResourceExtractor {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceExtractor.class);

  private final URLNormalizer urlNormalizer = new URLNormalizer();

  private final Pattern javascriptPattern = Pattern.compile("((\"|\')(([-a-zA-Z0-9+&@#/%?=~_|!:,;\\.])*)(\"|\'))");

  private static final int STACK_OVERFLOW_LIMIT = 6000;

  public Iterable<Resource> extractResources(String sourceUrl, String html) {
    if (sourceUrl == null) {
      return Collections.emptySet();
    }

    String prefixForInternalLinks = urlNormalizer.createPrefixForInternalLinks(sourceUrl);

    Set<Resource> resources = Sets.newHashSet();

    Document doc = Jsoup.parse(html);
    Elements iframes = doc.select("iframe[src]");
    Elements scripts = doc.select("script");
    Elements links = doc.select("link[href]");
    Elements imgs = doc.select("img[src]");

    Elements all = iframes.clone();
    all.addAll(scripts);
    all.addAll(links);
    all.addAll(imgs);

    for (Element tag: all) {
      String uri = tag.attr("src");

      if (!uri.contains(".")) {
        uri = tag.attr("href");
      }

      if (uri.contains(".")) {
          uri = urlNormalizer.expandIfInternalLink(prefixForInternalLinks, uri);
          // normalize link
          try {
              uri = urlNormalizer.normalize(uri);
              uri = urlNormalizer.extractDomain(uri);
          } catch (MalformedURLException e) {
              if (LOG.isWarnEnabled()) {
                  LOG.warn("Malformed URL: \"" + uri + "\"");
              }
          }
          if (isValidDomain(uri)) {
              resources.add(new Resource(uri, type(tag.tag().toString())));
          }
      }

      /*
        Handle all cases where the script element is built by Javascript:
        e.g:

        <!-- BEGIN GOOGLE ANALYTICS CODE -->
        <script type="text/javascript">
            //<![CDATA[
            (function(doc) {
                var ga = doc.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
                ga.src = ('https:' == doc.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js'; <!-- we need this url -->
                var s = doc.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
            })(document);
            //]]>
        </script>
        <!-- END GOOGLE ANALYTICS CODE -->
      */
      String script = tag.data();
      if (tag.tag().toString().equals("script") && script.length() < STACK_OVERFLOW_LIMIT){

        if (script.contains("src") || script.contains("CDATA") || script.contains(".post(") ||
            script.contains("url") && script.contains(".ajax") || script.contains("require")) {

          Matcher matcher = javascriptPattern.matcher(tag.data());
          while (matcher.find()) {

            for(int i = 0; i < matcher.groupCount(); i++) {
              String url = matcher.group(i);

              if (url != null && url.contains(".") && !url.contains("\"") && !url.contains("'")) {
                if (!url.contains(":") || url.contains("://")) {
                  try {
                    url = urlNormalizer.normalize(url);
                    url = urlNormalizer.extractDomain(url);
                    if (isValidDomain(url)) {
                      resources.add(new Resource(url, type(tag.tag().toString())));
                      break;
                    }
                  } catch (MalformedURLException e) {
                      if (LOG.isWarnEnabled()) {
                          LOG.warn("Malformed URL: \"" + uri + "\"");
                      }
                  }
                }
              }
            }
          }
        }
      }
    }

    return resources;
  }

  private boolean isValidDomain(String url) {
    if (!url.contains(".") || url.contains("///")) {
      return false;
    }

    int startTopLevelDomain = url.lastIndexOf('.');
    String topLevelDomain = url.substring(startTopLevelDomain + 1);
    DomainValidator dv = DomainValidator.getInstance();
    return dv.isValidTld(topLevelDomain);
  }

  private Resource.Type type(String tag) {
    if ("script".equals(tag)) {
      return Resource.Type.SCRIPT;
    }
    if ("link".equals(tag)) {
      return Resource.Type.LINK;
    }
    if ("img".equals(tag)) {
      return Resource.Type.IMAGE;
    }
    if ("iframe".equals(tag)) {
      return Resource.Type.IFRAME;
    }

    return Resource.Type.OTHER;
  }

}
