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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;

import com.google.common.collect.Sets;

public class ResourceExtractor {

  private final URLNormalizer urlNormalizer = new URLNormalizer();


  public Iterable<Resource> extractResources(String sourceUrl, String html) {

    if (sourceUrl == null) {
      return Collections.emptySet();
    }

    String prefixForInternalLinks = urlNormalizer.createPrefixForInternalLinks(sourceUrl);

    Set<Resource> resources = Sets.newHashSet();

    Document doc = Jsoup.parse(html);
    Elements iframes = doc.select("iframe[src]");
    Elements scripts = doc.select("script");
    Elements links   = doc.select("link[href]");
    Elements imgs    = doc.select("img[src]");

    Elements all = iframes.clone();
    all.addAll(scripts);
    all.addAll(links);
    all.addAll(imgs);


    for (Element tag: all) {
      String uri = tag.attr("src");

      if(!uri.contains(".")) {
          uri = tag.attr("href");
      }


      if(uri.contains(".")) {
          uri = urlNormalizer.expandIfInternalLink(prefixForInternalLinks, uri);
          // normalize link
          try {
              uri = urlNormalizer.normalize(uri);
              uri = urlNormalizer.extractDomain(uri);
          } catch (MalformedURLException e) {
              //System.out.println("Unable to process resource: " + uri);
          }
          if (uri.contains(".")) {
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
      //TODO: currently it is not possible to detect the domain if the src is concatenated by variables
      if (tag.tag().toString().equals("script")){
        if (tag.data().contains("src")) {
            String [] endTags = {";","type","%3E%3C",">"};
            String [] startTags = {"src","=",")"};

            int srcStart = tag.data().indexOf("src");
            String cDATA = tag.data().substring(srcStart);

            //first delete the part right to the url
            for (String endTag : endTags) {
                int srcEnd = cDATA.indexOf(endTag);
                if (srcEnd != -1) {
                    String temp = cDATA.substring(0, srcEnd);
                    if (temp.contains(".")) {
                        cDATA = temp;
                    }
                }
            }

            //first delete the part left to the url
            for (String endTag : endTags) {
                int srcEnd = cDATA.indexOf(endTag);
                if (srcEnd != -1) {
                    String temp = cDATA.substring(srcEnd+1);
                    if (temp.contains(".")) {
                        cDATA = temp;
                    }
                }
            }

            //delete right string separator
            int lastSeparator = Math.max(cDATA.lastIndexOf('\''), cDATA.lastIndexOf('\"'));
            if (lastSeparator != -1) {
                cDATA = cDATA.substring(0, lastSeparator);
            } else {
                continue;
            }

            //delete left string separator
            lastSeparator = Math.max(cDATA.lastIndexOf('\''),cDATA.lastIndexOf('\"'));
            cDATA = cDATA.substring(lastSeparator+1);

            try {
              cDATA = urlNormalizer.normalize(cDATA);
              cDATA = urlNormalizer.extractDomain(cDATA);
              if (cDATA.contains(".") && !cDATA.contains("///")) {
                resources.add(new Resource(cDATA, type(tag.tag().toString())));
              }
            } catch(Exception e) {}

        }
      }

    }

    return resources;
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
