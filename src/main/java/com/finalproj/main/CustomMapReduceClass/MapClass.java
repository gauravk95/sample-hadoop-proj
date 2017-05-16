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

import com.finalproj.main.DisplayController;
import com.finalproj.main.DataModels.RecordHolder;

/**************************************************************************************
 * Map Class which extends MaReduce.Mapper class
 * Map is passed a N lines at a time, it splits the line based o delem
 * and generated the token which are output by map with value as one to be consumed
 * by reduce class
 * @author Gaurav Kumar
 *************************************************************************************/
public class MapClass extends Mapper<LongWritable, Text, NullWritable, NullWritable>{
	 
	public static final int DATA_TYPE_INT = 1;
	public static final int DATA_TYPE_FLOAT = 2;
	public static final int DATA_TYPE_STRING = 0;
	
    public static int COL_INDEX = 0;
    
    //2 for String, by default
    public static int DATA_TYPE_INDEX = 0;
    
    public static final String COL_DELM = ",";
    public static final String ROW_DELM = "\n";
    
    public static String records;
        
    /**
     * map function of Mapper parent class takes a line of text at a time
     * splits to tokens and passes to the context as word along with value as one
     */
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		records= value.toString();
        	
	}


}
