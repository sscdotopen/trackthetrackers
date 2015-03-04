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

    if (nullOrEmpty("analysis.trackingraphsample.path")) {
      throw new IllegalStateException("[analysis.trackingraphsample.path] in conf.properties must point to " +
          "analysis/src/resources/sampleSeg.tsv");
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

    if (nullOrEmpty("phantomjs.path")) {
      throw new IllegalStateException("[phantomjs.path] in conf.properties must point to a local phantomjs binary, " +
          "get it from http://phantomjs.org/download.html");
    }

    if (nullOrEmpty("analysis.results.path") || !endsWith("analysis.results.path", "/")) {
      throw new IllegalStateException("[analysis.results.path] in conf.properties must point to a local directory " +
          "where the analysis results will be stored and end with a /");
    }
  }

  private static boolean nullOrEmpty(String key) {
    return props.getProperty(key) == null || "".equals(props.getProperty(key));
  }

  private static boolean endsWith(String key, String pattern) {
    return props.getProperty(key).endsWith(pattern);
  }
}