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

package io.ssc.trackthetrackers;

import java.util.Properties;

public class Config {

  private static Properties props;

  private Config() {}

  public static String get(String key) {
    try {
      if (props == null) {
        props = new Properties();
        props.load(Config.class.getResourceAsStream("/conf/conf.properties"));
        validateConfig();
      }
      return props.getProperty(key);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to load config file!", e);
    }
  }

  private static void validateConfig() {

    if (nullOrEmpty("commoncrawl.samples.path")) {
      throw new IllegalStateException("[commoncrawl.samples.path] in conf.properties must point to the folder " +
          "that contains the extraction/src/test/resources/commoncrawl folder");
    }

    if (nullOrEmpty("commoncrawl.problematic.path")) {
      throw new IllegalStateException("[commoncrawl.samples.path] in conf.properties must point to the folder " +
          "that contains the extraction/src/test/resources/commoncrawl folder");
    }

    if (nullOrEmpty("analysis.trackingraphsample.path")) {
      throw new IllegalStateException("[analysis.trackingraphsample.path] in conf.properties must point to " +
          "analysis/src/resources/sampleSeg.tsv");
    }

    if (nullOrEmpty("analysis.trackingraphminisample.path")) {
      throw new IllegalStateException("[analysis.trackingraphminisample.path] in conf.properties must point to " +
          "analysis/src/resources/sampleMini.tsv");
    }

    if (nullOrEmpty("webdatacommons.pldfile")) {
      throw new IllegalStateException("[webdatacommons.pldfile] in conf.properties must point to the webdata commons " +
          "domain index file, download it from " +
          "http://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/pld-index.gz");
    }

    if (nullOrEmpty("webdatacommons.pldfile.unzipped")) {
      throw new IllegalStateException("[webdatacommons.pldfile.unzipped] in conf.properties must point to the " +
          "unzipped webdata commons domain index file, download it from " +
          "http://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/pld-index.gz");
    }

    if (nullOrEmpty("webdatacommons.pldarcfile.unzipped")) {
      throw new IllegalStateException("[webdatacommons.pldarcfile.unzipped] in conf.properties must point to the " +
          "unzipped webdata commons hyperlink graph file, download it from " +
          "http://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/pld-arc.gz");
    }

    if (nullOrEmpty("phantomjs.path")) {
      throw new IllegalStateException("[phantomjs.path] in conf.properties must point to a local phantomjs binary, " +
          "get it from http://phantomjs.org/download.html");
    }

    if (nullOrEmpty("analysis.results.path") || !endsWith("analysis.results.path", "/")) {
      throw new IllegalStateException("[analysis.results.path] in conf.properties must point to a local directory " +
          "where the analysis results will be stored and end with a /");
    }

    if (nullOrEmpty("topleveldomainByCountry.csv")) {
      throw new IllegalStateException("[topleveldomainByCountry.csv] in conf.properties must point to a local csv file, " +
          "which matches toplevel domains with the corresponding country and can be found here: \n" + 
          "trackthetrackers/extraction/src/test/resources/unfolding/topleveldomainByCountry.csv");
    }

    if (nullOrEmpty("countries.geo.json")) {
      throw new IllegalStateException("[countries.geo.json] in conf.properties must point to a local json file, " +
          "which contains the geo information of country coordinates. Can be found here: \n" +
          "trackthetrackers/extraction/src/test/resources/unfolding/countries.geo.json\n" +
          "Or downloaded here: \n" +
          "https://raw.githubusercontent.com/tillnagel/unfolding/master/data/data/countries.geo.json");
    }

    if (nullOrEmpty("webdatacommons.hostgraph-pr.unzipped")) {
      throw new IllegalStateException("[webdatacommons.hostgraph-pr.unzipped] in conf.properties must point to a local csv file, " +
          "which contains a domain with its page rank. Can be downloaded here: \n" +
          "http://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/ranking/hostgraph-pr.tsv.gz\n");
    }

    if (nullOrEmpty("output.images") || !endsWith("output.images", "/")) {
      throw new IllegalStateException("[output.images] in conf.properties must point to a local folder, " +
          "which will contain snapshots of created charts\n" +
          "The name of the directory has to end with a /\n");
    }

    if (nullOrEmpty("company.distribution.by.country")) {
      throw new IllegalStateException("[company.distribution.by.country] in conf.properties must point to a local csv file, " +
         "which will contain a country with a corresponding value.\n" +
         "This is an intermediate result to be able to draw the world map.\n");
    }  
  }

  private static boolean nullOrEmpty(String key) {
    return props.getProperty(key) == null || "".equals(props.getProperty(key));
  }

  private static boolean endsWith(String key, String pattern) {
    return props.getProperty(key).endsWith(pattern);
  }
}
