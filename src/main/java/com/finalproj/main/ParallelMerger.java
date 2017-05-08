package com.finalproj.main;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*****************************************
 * Merges Multiple Record Holder into one
 * @author Gaurav Kumar
 *****************************************/

//TODO: ABove merger is not working properly...fix it

public class ParallelMerger {
	
	
	private List<RecordHolder> list;
	
	private int startIndex=0,endIndex=0;
	private boolean jobDone = false;
	
	 private ExecutorService executor;
	
	public ParallelMerger(List<RecordHolder> list)
	{
		this.list = list;
		int cores = 5;
		executor= Executors.newFixedThreadPool(5);
	}
	
	public RecordHolder merge(){
	
	RecordHolder finalSor = null;
	
		startIndex = 0;
		endIndex = list.size();
		
		//initially create N/2 merger threads
		initMerger();
		
		//repeat the task until only one record is left and the executor is terminated
		while(list.size()!=1&&!executor.isTerminated())
		{
								
		}
		
		finalSor = list.get(0);
		
		return finalSor;
	
	}
	
	
	private void initMerger() {
		// TODO Auto-generated method stub
	
		
		//only if there are 2 or more elements in the list to merge
		if((list.size())>=2)
		{
			int size = list.size();				
			for(int i = 0; i<size; i+=2)
			{
				//ensure that pairs are only used to assign threads
				//get 2 consecutive records
				RecordHolder r1 = (RecordHolder) list.get(i);
				RecordHolder r2 = (RecordHolder) list.get(i+1);
				
				Runnable worker = new MergerThread(r1.getMultipleRows(),r2.getMultipleRows());  
	            executor.execute(worker);
	            list.remove(i);
				list.remove(i);
			}
			
		
		}
		
	}
	
	private void createMergerThread()
	{
	
		int i = list.size()-2;
		//only if there are 2 or more elements in the list to merge
				if((list.size())>=2)
				{
					
					//ensure that pairs are only used to assign threads
					//get 2 consecutive records
					RecordHolder r1 = (RecordHolder) list.get(i);
					RecordHolder r2 = (RecordHolder) list.get(i+1);
					
					Runnable worker = new MergerThread(r1.getMultipleRows(),r2.getMultipleRows());  
			        executor.execute(worker);
			        
			        list.remove(i);
					list.remove(i);
				}

	}


	class MergerThread implements Runnable {  
	    private ArrayList<RecordRow> ar1;
	    private ArrayList<RecordRow> ar2;
	    
	    public MergerThread(ArrayList<RecordRow> ar1,ArrayList<RecordRow> ar2){  
	        this.ar1 = ar1;
	        this.ar2 = ar2;
	    } 
	    
	     public void run() {  
	        System.out.println("\n----------------------------------------------\n"+Thread.currentThread().getName()+" INFO: Started Execution, Merge Started");  
	        
	        mergeRecords();               
	        
	        System.out.println(Thread.currentThread().getName()+" INFO: End Execution, Merge Completed"+"\n----------------------------------------------");//prints thread name  
	    }  
	     

	 	private void mergeRecords()
	     {
	     	ArrayList<RecordRow> tempAr = new ArrayList<RecordRow>();
	     	System.out.println("#############  MERGING("+ar1.size()+","+ar2.size()+")  #################");
	         int i = 0;
	         int j = 0;
	      
	         // Copy the smallest values from either the left or the right side back
	         // to the original array
	         while (i < ar1.size() && j < ar2.size()) {
	                 if (ar1.get(i).getSingleCol(MapClass.COL_INDEX).compareTo(ar2.get(j).getSingleCol(MapClass.COL_INDEX))<0) {
	                         tempAr.add(ar1.get(i));
	                         i++;
	                 } else if (ar1.get(i).getSingleCol(MapClass.COL_INDEX).compareTo(ar2.get(j).getSingleCol(MapClass.COL_INDEX))==0) {
	                     tempAr.add(ar1.get(i));
	                 	tempAr.add(ar2.get(j));
	                         i++;
	                         j++;
	                 }
	                 else
	                 {
	                 	tempAr.add(ar2.get(j));
	                     j++;
	                 }
	               
	         }
	         // Copy the rest of the left side of the array into the target array
	         while (i < ar1.size()) {
	         		tempAr.add(ar1.get(i));
	                 i++;
	         }
	         
	         // Copy the rest of the left side of the array into the target array
	         while (j < ar2.size()) {
	         		tempAr.add(ar2.get(j));
	                 j++;
	         }

	         
	         //add the merged record to the list
				RecordHolder tHolder =new RecordHolder();
				tHolder.setMultipleRows(tempAr);
				
				list.add(tHolder);
					
							
				//once the previous merge is finished, create a new merger
				createMergerThread();
	     	
	     }
	     
	}  

}
