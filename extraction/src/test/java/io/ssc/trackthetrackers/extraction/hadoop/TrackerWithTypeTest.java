package io.ssc.trackthetrackers.extraction.hadoop;

import static org.junit.Assert.*;

import java.util.Set;

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

}
