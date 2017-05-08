package com.finalproj.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

/************************************************************************
 * Reduce class which is executed after the map class and takes
 * key(word) and corresponding values, merges the values
 * and writes to the output
 * 
 * @author Gaurav Kumar
 *************************************************************************/
public class ReduceClass extends Reducer<IntWritable, RecordHolder, Text, NullWritable>{

	private static NullWritable nullRec = NullWritable.get();
	
	/**
	 * Method which performs the reduce operation and sums 
	 * all the occurrences of the word before passing it to be stored in output
	 */
	@Override
	protected void reduce(IntWritable key, Iterable<RecordHolder> values,
			Context context)
			throws IOException, InterruptedException {
	/*
		int sum = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		
		while(valuesIt.hasNext()){
			sum = sum + valuesIt.next().get();
		}
		*/
		int k = 0;
		
		//change iterable to Collection
		ArrayList<RecordHolder> partRecord = new ArrayList<RecordHolder>();
		for (RecordHolder value : values) {
		        	RecordHolder row = new RecordHolder();
		        	row.setMultipleRows(value.getMultipleRows());
		        	partRecord.add(row);
		        	k++;
		        	
			}
			
				
		//TODO: Merge not working properly, FIX it
		ParallelMerger merger = new ParallelMerger(partRecord);
		RecordHolder mergedRec = merger.merge();
		
		System.out.println("\n***************************"+mergedRec.toString()+"\n*****************************************");
		
		context.write(new Text(mergedRec.toString()), nullRec);
		
		
	}	
	
}