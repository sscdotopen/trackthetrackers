package io.ssc.trackthetrackers.extraction.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.TwoDArrayWritable;

public class TwoDIntArrayWritable extends TwoDArrayWritable{

	
	public TwoDIntArrayWritable() {
        super(IntWritable.class);

    }


    public TwoDIntArrayWritable(Class valueClass) {
        super(valueClass);
        
    }
}

