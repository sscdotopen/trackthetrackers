package io.ssc.trackthetrackers.extraction.extraction;

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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.ssc.trackthetrackers.extraction.resources.Resource;
import io.ssc.trackthetrackers.extraction.resources.ResourceExtractor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;
import org.junit.Test;

import java.io.IOException;
import java.io.File;
import java.net.URL;


public class MiniBenchmark {

  private static long getScriptSize(URL res) throws IOException {
    Document doc = Jsoup.parse(Resources.toString(res, Charsets.UTF_8));
    Elements scripts = doc.select("script");
    
    long sum = 0L;
    
    for (Element script : scripts) {
      sum += script.data().length();
    }
    return sum;
  }

  @Test
  public void miniBenchmark() throws IOException {
    
    long runs = 1000L;
    String filename = "buzzfeed.com.html";

    URL res = Resources.getResource(filename);
    File html = new File(res.getFile());
    long fileSize = html.length();
    long scriptSize = getScriptSize(res);
    
    System.out.println("File name: " + filename);  
    System.out.println("File size: " + fileSize + "B");
    System.out.println("Script size: " + scriptSize + " characters\n");

    long startTime = System.nanoTime();
    for (long i = 0; i < runs; i++) {
      extractResources("http://buzzfeed.com", res);
    }
    long endTime = System.nanoTime();
    long duration = endTime - startTime;  //divide by 1000000 to get milliseconds.
    
    long durationInMS = duration / 1000000L;
    System.out.println("Runs: " + runs + " - execution time: " + durationInMS + "ms\n");
      
    System.out.println("ms per Run: " + (durationInMS / runs));
    System.out.println("File size(in kB) / ms: " + (((fileSize / 1024) * runs) / durationInMS));
    System.out.println("File size(in character) / ms: " +
        ((Resources.toString(res, Charsets.UTF_8).length() * runs) / durationInMS));
    System.out.println("Script size(in character) / ms: " + ((scriptSize * runs) / durationInMS));
  }  

  void extractResources(String sourceUrl, URL page) throws IOException {
    new ResourceExtractor().extractResources(sourceUrl, Resources.toString(page, Charsets.UTF_8));
  }

}
