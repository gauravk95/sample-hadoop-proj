package com.finalproj.main.TestMapReduceClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.finalproj.main.ParallelMerger;
import com.finalproj.main.RecordHolder;
import com.finalproj.main.RecordRow;
import com.google.common.collect.Lists;

/************************************************************************
 * Reduce class which is executed after the map class and takes
 * key(word) and corresponding values, merges the values
 * and writes to the output
 * 
 * This Class is uses hadoop's default technique: sort,shuffle, write based on sorting of key 
 * 
 * @author Gaurav Kumar
 *************************************************************************/
public class TestReduceClass extends Reducer<Text, Text, Text, NullWritable>{

	private static NullWritable nullRec = NullWritable.get();
	
	/**
	 * Method which performs the reduce operation and sums 
	 * all the occurrences of the word before passing it to be stored in output
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
	
		
		Iterator<Text> valuesIt = values.iterator();
		
		while(valuesIt.hasNext()){
			context.write(new Text(valuesIt.toString()), nullRec);
		}
				
		
	}	
	
}