package io.ssc.trackthetrackers.extraction.hadoop;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.extraction.hadoop.TrackerWithType;

import org.junit.Test;

import com.google.common.collect.Sets;

public class TrackerWithTypeTest {

	@Test
	public void uniqueSets() {
		
		TrackerWithType t1 = new TrackerWithType("a",100, -1);
		TrackerWithType t2 = new TrackerWithType("a",200, -1);
		TrackerWithType t3 = new TrackerWithType("a",100, -1);
		TrackerWithType t4 = new TrackerWithType("a",100, -2);
		
		//t1 and t3 should be equal and not added twice
		Set<TrackerWithType> s =  Sets.newHashSet();
		s.add(t1); s.add(t2); s.add(t3); s.add(t4);
		assertTrue(s.size()==3);
		
	}
	
	@Test
	public void sameOutput() {
		
		//will test if the tracked graph on the commoncrawl sample 
		//is the same before and after adding the trackingtype thmask
		
		//read the original file into a hashmap
		String file1Path = Config.get("trackingtype.sample.withmask");
		String file2Path = Config.get("trackingtype.sample.nomask"); 
		Set<String> s1 = buildSetFromFile(file1Path);
		Set<String> s2 = buildSetFromFile(file2Path);
		
		assertTrue("The two sets are not equal in size. s1 ="+s1.size()+" s2="+s2.size() , s1.size()==s2.size());
		assertTrue ("The two sets are equal in size but with different entries",s1.equals(s2));
		
		
	
		
	}
	
	
	
	private Set<String> buildSetFromFile (String filePath){
		
		try{
		BufferedReader br;
		Set<String> map= Sets.newHashSet();
			br = new BufferedReader(new FileReader(filePath));
	        String line = br.readLine();
	        
	        while (line != null) {
	        	
	        	String[] toks = line.split("\\s+");
	        	map.add(toks[0]+"#"+toks[1]);
	            line = br.readLine();
	        }

		br.close();
		return map;
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return null;
	
		
	}
	
	
	
	

}
