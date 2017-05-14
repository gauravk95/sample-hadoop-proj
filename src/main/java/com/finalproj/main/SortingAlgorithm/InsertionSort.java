package com.finalproj.main.SortingAlgorithm;

import java.util.ArrayList;
import java.util.Collections;

import com.finalproj.main.CustomMapReduceClass.MapClass;
import com.finalproj.main.DataModels.RecordRow;


//TODO: This method is not sorting the records....Fix it

public class InsertionSort {
	
	
	public static void sort(ArrayList<RecordRow> row)
	{
		//decide which data type comparison is required

		if(MapClass.DATA_TYPE_INDEX==MapClass.DATA_TYPE_INT)
		{
			//integer
			sortInt(row);
		}
		else if(MapClass.DATA_TYPE_INDEX==MapClass.DATA_TYPE_FLOAT)
		{
			//float
			sortFloat(row);
		}
		else
		{
			//string
			sortStr(row);
		}
	}
	
	/***************************************INSERTION SORT********************************/
	//Handles Float Using Insertion Sort
			public static  void sortInt(ArrayList<RecordRow> row) {  
				int n = row.size();  
		        for (int j = 1; j < n; j++) { 
		       
		            Integer key = row.get(j).getCompInteger();  
		            int i = j-1;  
		            while ( (i > -1) && ( row.get(i).getCompInteger() > key ) ) {  
		                
		            	//exchange the records
		            	Collections.swap(row, i, i+1);
		            	
		                i--;  
		            } 
		            
		          //exchange the records
	            	Collections.swap(row,j, i+1);
		           
		        }  
		        
		        System.out.println("\n****************************SORTED RECORD****************************\n"+row.toString());
		    }

	//Handles Float Using Insertion Sort
		public static void sortFloat(ArrayList<RecordRow> row) {  
			int n = row.size();  
	        for (int j = 1; j < n; j++) { 
	       
	            Float key = row.get(j).getCompFloat();  
	            int i = j-1;  
	            while ( (i > -1) && ( row.get(i).getCompFloat()>key ) ) {  
	                
	            	//exchange the records
	            	Collections.swap(row, i, i+1);
	            	
	                i--;  
	            } 
	            
	          //exchange the records
            	Collections.swap(row,j, i+1);
	           
	        }  
	        
	        System.out.println("\n****************************SORTED RECORD****************************\n"+row.toString());
	    }
		
		//Handles String Using Insertion Sort
				public static  void sortStr(ArrayList<RecordRow> row) {  
			        int n = row.size();  
			        for (int j = 1; j < n; j++) { 
			       
			            String key = row.get(j).getCompString();  
			            int i = j-1;  
			            while ( (i > -1) && ( row.get(i).getCompString().compareTo(key) > 0 ) ) {  
			                
			            	//exchange the records
			            	Collections.swap(row, i, i+1);
			            	
			                i--;  
			            } 
			            
			          //exchange the records
		            	Collections.swap(row,j, i+1);
			           
			        } 
			        
			        System.out.println("\n****************************SORTED RECORD****************************\n"+row.toString());
			      
			    }

				
}
