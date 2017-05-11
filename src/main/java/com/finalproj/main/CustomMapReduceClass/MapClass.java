package com.finalproj.main.CustomMapReduceClass;

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

/**************************************************************************************
 * Map Class which extends MaReduce.Mapper class
 * Map is passed a N lines at a time, it splits the line based o delem
 * and generated the token which are output by map with value as one to be consumed
 * by reduce class
 * @author Gaurav Kumar
 *************************************************************************************/
public class MapClass extends Mapper<LongWritable, Text, IntWritable, RecordHolder>{
	 
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
		temp.sortRecords(COL_INDEX);
		
		System.out.println("##########################################################\n"+temp.toString());
		
			context.write(new IntWritable(1),temp);
				
	}
	
	


}
