package io.ssc.trackthetrackers.extraction;


import com.google.common.collect.Maps;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class Indexing {

  public static void main(String[] args) throws IOException {

    String dataset = "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty.tsv";

    Map<String, Integer> index = Maps.newHashMapWithExpectedSize(45000000);
    int count = 1;

    try (BufferedReader reader = new BufferedReader(new FileReader(dataset))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] tokens = line.split("\t");

        int srcIndex = index.get(tokens[0]);

        System.out.println(tokens[0] + " <-- " + tokens[1]);
      }
    }

  }
}
