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

import java.util.*;


public class ResourceExtractor {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceExtractor.class);

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
          resources.add(new Resource(uri, type(element.tag().toString())));
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
      } catch (Exception e) {}
    }
    
    List<String> splittedUrlCandidates = findUrlsInCode(javaScriptUrlCandidates);
    
    resources.addAll(resourcesFromCandidates(splittedUrlCandidates));

    return resources;
  }

  private List<String> findUrlsInCode(List<String> candidateUrls) {

    List<String> urlsInCode = new ArrayList<String>();  

    for (String currentString : candidateUrls) {
      String [] splits = currentString.split("\"|'");
      
      for (String token : splits) {
        String tok = token.trim();
        if (URLHandler.couldBeUrl(tok)) {
          tok = tok.replace("\\.", ".");          
          urlsInCode.add(tok);
        }
      }
    }

    return urlsInCode;
  }

  private Set<Resource> resourcesFromCandidates(List<String> candidateUrls) {
    Set<Resource> resources = Sets.newHashSet();
    for (String url : candidateUrls) {
      try {
        url = URLHandler.extractHost(url);
        resources.add(new Resource(url, Resource.Type.SCRIPT));
      } catch (MalformedURLException e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Malformed URL: \"" + url + "\"");
        }
      }
    }
    return resources;
  }

  private void findUrlCandidates(Node currentNode, List<String> urlCandidates) {

    if (currentNode.isString()) {
      if (currentNode.getString().contains(".")) {
        urlCandidates.add(currentNode.getString().trim());
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
