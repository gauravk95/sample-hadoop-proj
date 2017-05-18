package com.finalproj.main;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jfree.ui.RefineryUtilities;

import com.finalproj.main.CustomMapReduceClass.MapClass;
import com.finalproj.main.CustomMapReduceClass.ReduceClass;
import com.finalproj.main.CustomRecordReader.CustomFileInputFormat;
import com.finalproj.main.DataModels.RecordHolder;
import com.finalproj.main.TestMapReduceClass.*;


/*****************************************************************
 * The entry point for the SmartSort program,
 * which setup the Hadoop job with Map and Reduce Class
 * 
 * @author Gaurav Kumar
 ******************************************************************/
public class MainController extends Configured implements Tool{
	
	public static int runType = 0;
	/**
	 * Main function which calls the run method and passes the args using ToolRunner
	 * @param args Two arguments input and output file paths
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MainController(), args);  
		//System.exit(exitCode);
	}
	
 
	/**
	 * Run method which schedules the Hadoop Job
	 * @param args Arguments passed in main function
	 */
	public int run(String[] args) throws Exception {
		
		if(args.length==1)
		{
			if(args[0].compareTo("-t")==0)
			{
				runType = 1;
			}
			else
			{
				System.err.printf("\nUsage: %s needs one/three arguments <input_file> <output_file> <options> \n\n \tOPTIONS: \n\t\t-t : Open in Test Mode\n\t\t-p: Open in production mode \n\n ",
						getClass().getSimpleName());
				return -1;
			}
		}
		else if(args.length==3)
		{
			if(args[0].compareTo("-p")==0)
			{
				runType = 0;
			}
			else
			{
				System.err.printf("\n*****Usage: %s needs one/three arguments <input_file> <output_file> <options> \n\n \tOPTIONS: \n\t\t-t : Open in Test Mode\n\t\t-p: Open in production mode \n\n ",
						getClass().getSimpleName());
				return -1;
			}
		}
		else
		{
			System.err.printf("\n*****Usage: %s needs one/three arguments <input_file> <output_file> <options> \n\n \tOPTIONS: \n\t\t-t : Open in Test Mode\n\t\t-p: Open in production mode \n\n ",
					getClass().getSimpleName());
			return -1;
		}
		
		int returnValue = 0;
		
		if(runType==0)
		{	
			//Initialize the Hadoop job and set the jar as well as the name of the Job
			Job job = new Job();
			job.setJarByClass(MainController.class);
			job.setJobName("SmartSort");
			
			//Add input and output file paths to job based on the arguments passed
			CustomFileInputFormat.addInputPath(job, new Path(args[1]));
			job.setInputFormatClass(CustomFileInputFormat.class);
			
			//remove the output directory
			 // Delete output if exists
		    FileSystem hdfs = FileSystem.get(this.getConf());
		    if (hdfs.exists(new Path(args[2])))
		      hdfs.delete(new Path(args[2]), true);
			
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			
				//normal mode config
				job.setMapOutputKeyClass(NullWritable.class);
				job.setMapOutputValueClass(NullWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				job.setMapperClass(MapClass.class);
	
				job.setNumReduceTasks(0);
				//job.setReducerClass(ReduceClass.class);
		
		
			//Wait for the job to complete and print if the job was successful or not
			returnValue = job.waitForCompletion(true) ? 0:1;
			
			if(job.isSuccessful()) {
				System.out.println("Map Job was successful");
				
				
				DisplayController display = new DisplayController();  
				display.initSortController();
				
			} else if(!job.isSuccessful()) {
				System.out.println("Map Job was not successful...Exiting Program...!");			
			}
		
		}
		else
		{

			DisplayController display = new DisplayController();  
			display.initSortController();
		}
		
		return returnValue;
	}
}
