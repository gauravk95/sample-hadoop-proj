package com.finalproj.main.CustomMapReduceClass;

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
import com.finalproj.main.DataModels.RecordHolder;
import com.finalproj.main.DataModels.RecordRow;
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
		
		//change iterable to Collection
		ArrayList<RecordHolder> partRecord = new ArrayList<RecordHolder>();
		for (RecordHolder value : values) {
		        	RecordHolder row = new RecordHolder();
		        	row.setMultipleRows(value.getMultipleRows());
		        	partRecord.add(row);
		        	
		        	System.out.println("-------------------------------------------\n"+row.toString());
		        	
			}
			
				
		//TODO: Merge not working properly, FIX it
		ParallelMerger merger = new ParallelMerger(partRecord);
		RecordHolder mergedRec = merger.merge();
		
		System.out.println("\n############################################\nMerging Completed!");
		//System.out.println("\n***************************"+mergedRec.toString()+"\n*****************************************");
		
		for(RecordRow temp : mergedRec.getMultipleRows())
			context.write(new Text(temp.toString()), nullRec);
		
		
	}	
	
}