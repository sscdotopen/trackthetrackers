/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz
 * <p/>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.extraction.resources;

import com.google.common.collect.Sets;
import com.google.javascript.jscomp.parsing.ParserRunner;
import com.google.javascript.rhino.Node;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ResourceExtractor {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceExtractor.class);

  private static final Pattern javascriptPattern =
      Pattern.compile("((\"|\')(([-a-zA-Z0-9+&@#/%?=~_|!:,;\\.])*)(\"|\'))");

  private final JavascriptParser javascriptParser = new JavascriptParser();


  public Iterable<Resource> extractResources(String sourceUrl, String html) {

    Set<Resource> resources = Sets.newHashSet();
    String prefixForInternalLinks = URLHandler.createPrefixForInternalLinks(sourceUrl);

    List<Element> elements = new ArrayList<Element>();

    Document doc = Jsoup.parse(html);
    Elements scripts = doc.select("script");

    elements.addAll(doc.select("iframe[src]"));
    elements.addAll(doc.select("link[href]"));
    elements.addAll(doc.select("img[src]"));
    elements.addAll(scripts);

    String uri;

    for (Element element : elements) {
      uri = element.attr("src").trim();

      if (!uri.contains(".")) {
        uri = element.attr("href").trim();
      }

      if (uri.contains(".")) {
        uri = URLHandler.expandIfInternalLink(prefixForInternalLinks, uri);
        try {
          uri = URLHandler.extractHost(uri);
          if (URLHandler.isValidDomain(uri)) {
            resources.add(new Resource(uri, type(element.tag().toString())));
          }
        } catch (MalformedURLException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Malformed URL: \"" + uri + "\"");
          }
        }
      }
    }

    List<String> javaScriptUrlCandidates = new ArrayList<String>();
    for (Element script : scripts) {
      try {
        String scriptContents = script.data();
        if (scriptContents.length() > 1) {
          ParserRunner.ParseResult parseResult = javascriptParser.parse(scriptContents);
          findUrlCandidates(parseResult.ast, javaScriptUrlCandidates);
        }
      } catch (Exception e) {
      }
    }

    findUrlsInCode(javaScriptUrlCandidates);

    resources.addAll(resourcesFromCandidates(javaScriptUrlCandidates));

    return resources;
  }

  private void findUrlsInCode(List<String> candidateUrls) {
    List<String> urlsInCode = new ArrayList<String>();

    Iterator<String> iterator = candidateUrls.iterator();
    while (iterator.hasNext()) {

      String currentString = iterator.next();

      if (currentString.contains("\"") || currentString.contains("'")) {
        Matcher matcher = javascriptPattern.matcher("'" + currentString + "'");
        boolean removedUponFind = false;
        while (matcher.find()) {
          if (!removedUponFind) {
            removedUponFind = true;
            iterator.remove();
          }

          for (int groupIndex = 0; groupIndex < matcher.groupCount(); groupIndex++) {
            String token = matcher.group(groupIndex);

            if (token != null && !token.contains("\"") && !token.contains("'") && URLHandler.couldBeUrl(token)) {
              urlsInCode.add(token.trim());
            }
          }
        }
      }
    }

    candidateUrls.addAll(urlsInCode);
  }

  private Set<Resource> resourcesFromCandidates(List<String> candidateUrls) {
    Set<Resource> resources = Sets.newHashSet();
    for (String url : candidateUrls) {
      if (URLHandler.couldBeUrl(url.trim())) {
        try {
          url = URLHandler.extractHost(url);

          if (URLHandler.isValidDomain(url)) {
            resources.add(new Resource(url, Resource.Type.SCRIPT));
          }
        } catch (MalformedURLException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Malformed URL: \"" + url + "\"");
          }
        }
      }
    }
    return resources;
  }

  private void findUrlCandidates(Node currentNode, List<String> urlCandidates) {

    if (currentNode.isString()) {
      if (currentNode.getString().contains(".")) {
        urlCandidates.add(currentNode.getString());
      }
    }

    for (Node child : currentNode.children()) {
      findUrlCandidates(child, urlCandidates);
    }
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
