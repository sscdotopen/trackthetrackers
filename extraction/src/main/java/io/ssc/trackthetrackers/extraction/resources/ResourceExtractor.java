package io.ssc.trackthetrackers.extraction.resources;

import com.google.common.collect.Sets;
import com.google.javascript.jscomp.parsing.Config;
import com.google.javascript.jscomp.parsing.Config.LanguageMode;
import com.google.javascript.jscomp.parsing.ParserRunner;
import com.google.javascript.rhino.ErrorReporter;
import com.google.javascript.rhino.Node;
import com.google.javascript.rhino.jstype.SimpleSourceFile;
import com.google.javascript.rhino.jstype.StaticSourceFile;
import org.apache.commons.validator.routines.DomainValidator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ResourceExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceExtractor.class);

    private final URLNormalizer urlNormalizer = new URLNormalizer();

    private final Pattern javascriptPattern = Pattern.compile("((\"|\')(([-a-zA-Z0-9+&@#/%?=~_|!:,;\\.])*)(\"|\'))");

    private static Set<String> EXTRA_ANNOTATIONS = new HashSet<String>(Arrays.asList(
            "suppressReceiverCheck",
            "suppressGlobalPropertiesCheck"
    ));


    public synchronized Iterable<Resource> extractResources(String sourceUrl, String html) {

        ArrayList<String> scriptHtml = new ArrayList<String>();

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

        for (Element tag: all) {
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
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Malformed URL: \"" + uri + "\"");
                    }
                } catch(StackOverflowError err){
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Stack Overflow Error: \"" + uri + "\"");
                    }
                }
                if (isValidDomain(uri)) {
                    resources.add(new Resource(uri, type(tag.tag().toString())));
                }
            }


            if (tag.tag().toString().equals("script")) { //filter functions
                if(tag.data().length() > 1) {
                    scriptHtml.add(tag.data());
                }
            }
        }

        Config config = ParserRunner.createConfig(
                true, LanguageMode.ECMASCRIPT5_STRICT, true, EXTRA_ANNOTATIONS);

        ErrorReporter errorReporter = new ErrorReporter() {
            @Override
            public void warning(String message, String sourceName, int line, int lineOffset) {
                // Ignore.
            }
            @Override
            public void error(String message, String sourceName, int line, int lineOffset) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Parser Error: \"" + message + "\"");
                }
            }
        };

        StaticSourceFile f = new SimpleSourceFile("input", false);

        ArrayList<String> parsedStrings = new ArrayList<String>();

        for(String script:scriptHtml) {
                ArrayList<String> variables = new ArrayList<String>();
                ArrayList<String> docVariables = new ArrayList<String>();
                try {
                    ParserRunner.ParseResult r = ParserRunner.parse(f, script, config, errorReporter);
                    //printTree(r.ast, 0);

                    parseStrings(r.ast, parsedStrings);
                } catch(Exception e) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Parser Exception: \"" + e + "\"");
                    }
                }
        }

        tokenizeStrings(parsedStrings); //get strings within strings

        resources.addAll(filterResourcesFromStrings(parsedStrings)); // check whether strings are actual urls

        return resources;
    }

    //parse url when it is within the string
    private void tokenizeStrings(ArrayList<String> parsedStrings) {

        ArrayList<Integer> toBeReplaced = new ArrayList<Integer>();

        ArrayList<String> tokenizedStrings = new ArrayList<String>();

        for(int o=0;o<parsedStrings.size();o++) {
            String currentString = parsedStrings.get(o);

            if( currentString.contains("\"") || currentString.contains("'")) {
                Matcher matcher = javascriptPattern.matcher("'" + currentString + "'");
                boolean found = false;
                while (matcher.find()) {
                    if (found == false) {
                        found = true;
                        toBeReplaced.add(o);
                    }

                    for(int i = 0; i < matcher.groupCount(); i++) {
                        String token = matcher.group(i);

                        if (token != null && !token.contains("\"") && !token.contains("'") && isUrl(token)) {
                            tokenizedStrings.add(token);
                        }
                    }
                }
            }
        }

        for(int o=toBeReplaced.size()-1;o>=0;o--) {
            parsedStrings.remove((int)toBeReplaced.get(o));
        }

        parsedStrings.addAll(tokenizedStrings);
    }


    private Set<Resource> filterResourcesFromStrings(ArrayList<String> parsedStrings) {
        Set<Resource> resources = Sets.newHashSet();
        for (String url : parsedStrings) {
            if (isUrl(url)) {
                // normalize link
                try {
                    url = urlNormalizer.normalize(url);
                    url = urlNormalizer.extractDomain(url);
                } catch (MalformedURLException e) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Malformed URL: \"" + url + "\"");
                    }
                } catch(StackOverflowError err){
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Stack Overflow Error: \"" + url + "\"");
                    }
                }
                if (isValidDomain(url)) {
                    resources.add(new Resource(url, Resource.Type.SCRIPT));
                }
            }
        }
        return resources;
    }

    private boolean isUrl(String url) {

        if(!url.contains(".")) {
            return false;
        }

        //remove and check white space
        url = url.trim();
        if(url.contains(" ") || url.contains("\t") || url.contains("\r") || url.contains("\n")) {
            return false;
        }

        //TODO: check this condition
        //this doesnt work for something like localhost:80/...
        if(url.contains(":")) {
            if(url.indexOf(':') < url.length() - 1 && url.charAt(url.indexOf(':')+1) != '/') {
                return false;
            }
        }

        return true;
    }

    private void printTree(Node root, int level) {
        for(int i = 0; i < level; i++) {
            System.out.print("\t");
        }
        System.out.println(root);

        for(Node child : root.children()){
            printTree(child,level+1);
        }
    }

    private void parseStrings(Node root, ArrayList<String> parsedStrings) {
        if(root.isString()) {
            if(root.getString().contains(".")) {
                parsedStrings.add(root.getString());
            }
        }

        for(Node child : root.children()){
            parseStrings(child, parsedStrings);
        }
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
