package com.finalproj.main;

import java.util.ArrayList;

import com.finalproj.main.CustomMapReduceClass.MapClass;
import com.finalproj.main.DataModels.RecordHolder;

public class SortingController {
	
	public final static int INSERTION_SORT = 1 ;
	public final static int MERGE_SORT = 2 ;
	public final static int QUICK_SORT = 3 ;
	public final static int HEAP_SORT = 4 ;
	public final static int SHELL_SORT = 5 ;
	public final static int TIM_SORT = 6 ;
	public final static int COUNTING_SORT = 7 ;
	public final static int RADIX_SORT = 8 ;
	public final static int ADAPTIVE_SORT = 9 ;
	
	public final static int N_RUNS= 1000;
	
	private RecordHolder mainRecords;
	
	public ArrayList<Long> execTimes;
	
	
	public SortingController()
	{
		System.out.println("INFO: Initaializing Sorting Controller");
		this.execTimes = new ArrayList<Long>();		
		this.mainRecords = new RecordHolder(MapClass.records,MapClass.ROW_DELM,MapClass.COL_DELM);
		for(int i=SortingController.INSERTION_SORT;i<=SortingController.ADAPTIVE_SORT;i++)
		{
			execTimes.add(0L);
		}
	}
	
	public void startSorting()
	{
		execTimes.clear();
		
		System.out.println("INFO: Starting sort procedure...");
		System.out.println("INFO: Generating Execution Time for different sort algorithm...");
		
		for(int i=SortingController.INSERTION_SORT;i<=SortingController.ADAPTIVE_SORT;i++)
		{
			//skip radix and counting sort for float and string type
			if((MapClass.DATA_TYPE_INDEX==MapClass.DATA_TYPE_FLOAT||MapClass.DATA_TYPE_INDEX==MapClass.DATA_TYPE_STRING)&&
					(i==RADIX_SORT||i==COUNTING_SORT))
			{	
				System.out.println("INFO: Skipping "+getSortingAlgorithmName(i)+" as its not applicable for Float/String Datatype");
				execTimes.add(0L);
				continue;	
			}
			
			long nexectime = 0;
			 System.out.println("INFO: Using "+getSortingAlgorithmName(i)+" to sort  data");
			
			 for(int j=0;j<N_RUNS;j++)
			{
			
			this.mainRecords = new RecordHolder(MapClass.records,MapClass.ROW_DELM,MapClass.COL_DELM);
			
			long startTime =System.nanoTime();
	           
			//sorts the records
			this.mainRecords.sortRecords(i);
			
			long endTime = System.nanoTime();
			
			nexectime+=(endTime-startTime);
			}
			
			System.out.println("INFO: Sorting Completed! Execution Time: "+(nexectime/N_RUNS));
			execTimes.add(nexectime/N_RUNS);
		
		}
		
		//System.out.println("\n****************************SORTED RECORD****************************\n"+this.mainRecords.toString());
			
	}
	
	public String[] getColumnNames()
	{
		return mainRecords.getColNames().getMultipleCols();
	}
	
	
	public ArrayList<Long> getExecTimes() {
		return execTimes;
	}

	public void setExecTimes(ArrayList<Long> execTimes) {
		this.execTimes = execTimes;
	}

	public static String getSortingAlgorithmName(int code)
	{
		String name="";
		switch(code)
		{
		case INSERTION_SORT: name = "Insertion Sort";
				break;
		case MERGE_SORT: name = "Merge Sort";
				break;
		case QUICK_SORT: name = "Quick Sort";
				break;
		case HEAP_SORT: name = "Heap Sort";
				break;
		case SHELL_SORT: name = "Shell Sort";
				break;
		case TIM_SORT: name = "Tim Sort";
				break;
		case COUNTING_SORT: name = "Counting Sort";
				break;
		case RADIX_SORT: name = "Radix Sort";
				break;
		case ADAPTIVE_SORT: name = "Adaptive Sort";
				break;
				
		}
		
		return name;
	}


}
