/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.ssc.trackthetrackers.extraction.resources;

import java.net.URL;
import java.net.MalformedURLException;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.Perl5Pattern;
import org.apache.oro.text.regex.Perl5Substitution;
import org.apache.oro.text.regex.Util;

/** Converts URLs to a normal form . */
class URLNormalizer {

    private Perl5Compiler compiler = new Perl5Compiler();
    private ThreadLocal<Perl5Matcher> matchers = new ThreadLocal<Perl5Matcher>() {
        protected Perl5Matcher initialValue() {
          return new Perl5Matcher();
        }
      };
    private final Rule relativePathRule;
    private final Rule leadingRelativePathRule;
    private final Rule currentPathRule;
    private final Rule adjacentSlashRule;

    public URLNormalizer() {
      try {
        // this pattern tries to find spots like "/xx/../" in the url, which
        // could be replaced by "/" xx consists of chars, different then "/"
        // (slash) and needs to have at least one char different from "."
        relativePathRule = new Rule();
        relativePathRule.pattern = (Perl5Pattern)
          compiler.compile("(/[^/]*[^/.]{1}[^/]*/\\.\\./)",
                           Perl5Compiler.READ_ONLY_MASK);
        relativePathRule.substitution = new Perl5Substitution("/");

        // this pattern tries to find spots like leading "/../" in the url,
        // which could be replaced by "/"
        leadingRelativePathRule = new Rule();
        leadingRelativePathRule.pattern = (Perl5Pattern)
          compiler.compile("^(/\\.\\./)+", Perl5Compiler.READ_ONLY_MASK);
        leadingRelativePathRule.substitution = new Perl5Substitution("/");

        // this pattern tries to find spots like "/./" in the url,
        // which could be replaced by "/"
        currentPathRule = new Rule();
        currentPathRule.pattern = (Perl5Pattern)
          compiler.compile("(/\\./)", Perl5Compiler.READ_ONLY_MASK);
        currentPathRule.substitution = new Perl5Substitution("/");

        // this pattern tries to find spots like "xx//yy" in the url,
        // which could be replaced by a "/"
        adjacentSlashRule = new Rule();
        adjacentSlashRule.pattern = (Perl5Pattern)      
          compiler.compile("/{2,}", Perl5Compiler.READ_ONLY_MASK);     
        adjacentSlashRule.substitution = new Perl5Substitution("/");
        
      } catch (MalformedPatternException e) {
        throw new RuntimeException(e);
      }
    }

  public String expandIfInternalLink(String prefixForInternalLinks, String link) {
   if (!link.startsWith("http") && !link.startsWith("ftp") && !link.startsWith("https")) {
     if (!link.startsWith("//")) {
         return prefixForInternalLinks + link;
     } else {
         return "http:" + link;
     }

   }
   return link;
  }

  public String removeQuestionMark(String url){

      int startQuestionMark = url.indexOf('?');
      if (startQuestionMark == -1) {
          return url;
      }
      return url.substring(0,startQuestionMark);
  }


  
  public String extractDomain(String url) {
      // extract domain from url

      //remove question mark operator in the case of e.g. php
      url = removeQuestionMark(url);

      if(url.matches("((ht|f)tp(s?))://((www.)?)((\\w|\\W){1,})/(.*)")) {
          url = url.substring(url.indexOf("://") + 3);
    	  url = url.substring(0, url.indexOf("/"));
      }

      return url;
  }

  public String createPrefixForInternalLinks(String sourceUrl) {
    String prefixForInternalLinks = sourceUrl;

    int pos = sourceUrl.lastIndexOf('/');
    if (pos == -1) {
      prefixForInternalLinks += '/';
    } else if (pos != prefixForInternalLinks.length() - 1) {
      prefixForInternalLinks = prefixForInternalLinks.substring(0, pos + 1);
    }
    return prefixForInternalLinks;
  }

    private String deletePHP(String url) {
        if(url.contains("?")) {
            return url.substring(0, url.indexOf('?'));
        }
        return url;
    }

    public String normalize(String urlString)
            throws MalformedURLException {

        urlString = urlString.trim();

        urlString = deletePHP(urlString);


        if ("".equals(urlString))                     // permit empty
            return urlString;

        urlString = urlString.trim();                 // remove extra spaces

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

        URL url = new URL(urlString);

        String protocol = url.getProtocol();
        String host = url.getHost();
        int port = url.getPort();
        String file = url.getFile();

        boolean changed = false;

        if (!urlString.startsWith(protocol))        // protocol was lowercased
            changed = true;

        if ("https".equals(protocol) || "http".equals(protocol) || "ftp".equals(protocol)) {

            if (host != null) {
                String newHost = host.toLowerCase();    // lowercase host
                if (!host.equals(newHost)) {
                    host = newHost;
                    changed = true;
                }
            }

            if (port == url.getDefaultPort()) {       // uses default port
                port = -1;                              // so don't specify it
                changed = true;
            }

            if (file == null || "".equals(file)) {    // add a slash
                file = "/";
                changed = true;
            }

            if (url.getRef() != null) {                 // remove the ref
                changed = true;
            }

            // check for unnecessary use of "/../"
            String file2 = substituteUnnecessaryRelativePaths(file);

            if (!file.equals(file2)) {
                changed = true;
                file = file2;
            }

        }

        if (changed)
            urlString = new URL(protocol, host, port, file).toString();

        return urlString;
    }

    private String substituteUnnecessaryRelativePaths(String file) {
        String fileWorkCopy = file;
        int oldLen = file.length();
        int newLen = oldLen - 1;

        // All substitutions will be done step by step, to ensure that certain
        // constellations will be normalized, too
        //
        // For example: "/aa/bb/../../cc/../foo.html will be normalized in the
        // following manner:
        //   "/aa/bb/../../cc/../foo.html"
        //   "/aa/../cc/../foo.html"
        //   "/cc/../foo.html"
        //   "/foo.html"
        //
        // The normalization also takes care of leading "/../", which will be
        // replaced by "/", because this is a rather a sign of bad webserver
        // configuration than of a wanted link.  For example, urls like
        // "http://www.foo.com/../" should return a http 404 error instead of
        // redirecting to "http://www.foo.com".
        //
        Perl5Matcher matcher = matchers.get();

        while (oldLen != newLen) {
            // substitue first occurence of "/xx/../" by "/"
            oldLen = fileWorkCopy.length();
            fileWorkCopy = Util.substitute
              (matcher, relativePathRule.pattern,
               relativePathRule.substitution, fileWorkCopy, 1);

            // remove leading "/../"
            fileWorkCopy = Util.substitute
              (matcher, leadingRelativePathRule.pattern,
               leadingRelativePathRule.substitution, fileWorkCopy, 1);

            // remove unnecessary "/./"
            fileWorkCopy = Util.substitute
            (matcher, currentPathRule.pattern,
            		currentPathRule.substitution, fileWorkCopy, 1);
            
            
            // collapse adjacent slashes with "/"
            fileWorkCopy = Util.substitute
            (matcher, adjacentSlashRule.pattern,
              adjacentSlashRule.substitution, fileWorkCopy, 1);
            
            newLen = fileWorkCopy.length();
        }

        return fileWorkCopy;
    }


    /**
     * Class which holds a compiled pattern and its corresponding substition
     * string.
     */
    private static class Rule {
        public Perl5Pattern pattern;
        public Perl5Substitution substitution;
    }

}

