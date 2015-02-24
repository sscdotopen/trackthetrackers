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

package io.ssc.trackthetrackers.extraction.resources;

import com.google.common.collect.Sets;
import io.ssc.trackthetrackers.Config;
import org.apache.commons.validator.routines.DomainValidator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.Closeables;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Set;
import java.util.Scanner;

public class GhostDriverExtractor {

  private static final Logger log = LoggerFactory.getLogger(GhostDriverExtractor.class);

  private final URLNormalizer urlNormalizer = new URLNormalizer();

  public Iterable<Resource> extractResources(String sourceUrl, String html) {

    Set<Resource> resources = Sets.newHashSet();
    String prefixForInternalLinks = urlNormalizer.createPrefixForInternalLinks(sourceUrl);

    Document doc = Jsoup.parse(html);
    Elements iframes = doc.select("iframe[src]");
    Elements links = doc.select("link[href]");
    Elements imgs = doc.select("img[src]");
    Elements scripts = doc.select("script");

    Elements all = iframes.clone();
    all.addAll(scripts);
    all.addAll(links);
    all.addAll(imgs);

    String uri;

    for (Element tag : all) {
      uri = tag.attr("src");

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
          if (log.isWarnEnabled()) {
            log.warn("Malformed URL: \"" + uri + "\"");
          }
        } catch (StackOverflowError err) {
          if (log.isWarnEnabled()) {
            log.warn("Stack Overflow Error: \"" + uri + "\"");
          }
        }
        if (isValidDomain(uri)) {
          resources.add(new Resource(uri, type(tag.tag().toString())));
        }
      }
    }

    BufferedWriter writer = null;
    File temp = null;
    try {

      //create a temporary html source file
      temp = File.createTempFile(sourceUrl, ".html");

      writer = new BufferedWriter(new FileWriter(temp));
      writer.write(html);

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      Closeables.closeAndSwallowIOExceptions(writer);
    }


    File tempLog = null;
    do {
      try {
        tempLog = File.createTempFile("log", ".log");
        tempLog.createNewFile();
      } catch (Exception e) {
        e.printStackTrace();
      }
    } while (!tempLog.exists());

    PhantomJSDriverService service = new PhantomJSDriverService.Builder()
        .usingPhantomJSExecutable(new File(Config.get("phantomjs.path"))).build();
    
    DesiredCapabilities capabilities = new DesiredCapabilities().phantomjs();
    capabilities.setCapability("phantomjs.settings.loadImages", false);
    
    PhantomJSDriver phantom = new PhantomJSDriver(service, capabilities);
    
    phantom.executePhantomJS(
        "var page      = this;\n" +
        "var filename = '" + tempLog.getAbsolutePath() + "';\n" +
        "var fs = require('fs');\n" +
        "page.onResourceRequested = function (requestData, networkRequest) {\n" +
        "    var content;" +
        "    var isLoaded;" +
        "    do {\n" +
        "      isLoaded = true;\n" +
        "      try {" +
        "        content = fs.read(filename);\n" +
        "      } catch (e) {\n" +
        "        isLoaded = false\n" +
        "     }" +
        "    } while (!isLoaded);"+
        "    fs.write(filename, content + requestData.url + ' ', 'w');\n" +
        "};\n" +
        "");

    phantom.get("file://" + temp.getAbsolutePath());

    Scanner scanner = null;
    try {
      scanner = new Scanner(tempLog);
      while (scanner.hasNextLine()) {
        String[] tokens = scanner.nextLine().split(" ");

        for (String url : tokens) {
          if (url.contains(".")) {
            if (url.startsWith("file://")) {
              url = url.substring(7);
              url = "http://" + url;
            }
            // normalize link
            try {
              url = urlNormalizer.normalize(url);
              url = urlNormalizer.extractDomain(url);
            } catch (MalformedURLException e) {
              if (log.isWarnEnabled()) {
                log.warn("Malformed URL: \"" + url + "\"");
              }
            } catch (StackOverflowError err) {
              if (log.isWarnEnabled()) {
                log.warn("Stack Overflow Error: \"" + url + "\"");
              }
            }
            if (isValidDomain(url)) {
              resources.add(new Resource(url, Resource.Type.SCRIPT));
            }
          }
        }
      }
    } catch (IOException e) {
      System.out.println(e.getStackTrace());
    } finally {
      temp.delete();
      tempLog.delete();
      Closeables.closeAndSwallowIOExceptions(scanner);
    }
    phantom.close();
    phantom.quit();
    
    return resources;
  }

  private boolean isValidDomain(String url) {
    if (!url.contains(".") || url.contains("///")) {
      return false;
    }
  
    if (url.contains(";") || url.contains("=") || url.contains("?")) {
      return false;
    }
  
    int startTopLevelDomain = url.lastIndexOf('.');
    String topLevelDomain = url.substring(startTopLevelDomain + 1);
    return DomainValidator.getInstance().isValidTld(topLevelDomain);
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
