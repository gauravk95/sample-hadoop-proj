package com.finalproj.main.TestMapReduceClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.finalproj.main.DataModels.RecordHolder;
import com.finalproj.main.DataModels.RecordRow;

/**************************************************************************************
 * Map Class which extends MaReduce.Mapper class
 * Map is passed a N lines at a time, it splits the line based o delem
 * and generated the token which are output by map with value as one to be consumed
 * by reduce class
 * 
 * Uses (K,V) pair...Considers the column data to be sorted as key, and value to be written as value
 * 
 * @author Gaurav Kumar
 *************************************************************************************/
public class TestMapClass extends Mapper<LongWritable, Text, Text, Text>{
	 
    public static final int COL_INDEX = 1;
    
    public static final String COL_DELM = ",";
    public static final String ROW_DELM = "\n";
    
    /**
     * map function of Mapper parent class takes a line of text at a time
     * splits to tokens and passes to the context as word along with value as one
     */
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String record= value.toString();
	
		RecordHolder temp = new RecordHolder(record,ROW_DELM,COL_DELM);	
		
		//sorts the records
		//temp.sortRecords(COL_INDEX);
		
		System.out.println("##########################################################\n"+temp.toString());
		
		for(RecordRow t : temp.getMultipleRows())
			context.write(new Text(t.getSingleCol(COL_INDEX)),new Text(t.toString()));
		
				
	}
	
	


}
