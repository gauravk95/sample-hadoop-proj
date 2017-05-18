package com.finalproj.main.DataModels;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Writable;

import com.finalproj.main.MainController;
import com.finalproj.main.SortingController;
import com.finalproj.main.CustomMapReduceClass.MapClass;
import com.finalproj.main.SortingAlgorithm.CountingSort;
import com.finalproj.main.SortingAlgorithm.HeapSort;
import com.finalproj.main.SortingAlgorithm.InsertionSort;
import com.finalproj.main.SortingAlgorithm.MergeSort;
import com.finalproj.main.SortingAlgorithm.QuickSort;
import com.finalproj.main.SortingAlgorithm.RadixSort;
import com.finalproj.main.SortingAlgorithm.ShellSort;

/**********************************************************************************
 * Holds N records/rows as a list of RecordRow, consists methods to  operate on them 
 * @author Gaurav Kumar
 **********************************************************************************/

public class RecordHolder implements Writable{
	
	private ArrayList<RecordRow> rows;
	
	private RecordRow colNames;
	
	public RecordHolder()
	{
		
	}
	
	public RecordHolder(String trows,String rowRegex,String colRegex) {
		String rowslist[] = trows.split(rowRegex);
		this.rows = new ArrayList<RecordRow>();
		
		//initialize the column name list,always string
		this.colNames = new RecordRow(rowslist[0],colRegex,true);
	
		try
		{
		//initialize the actual data
		for(int i =1;i<rowslist.length;i++)
		{
			this.rows.add(new RecordRow(rowslist[i],colRegex));
		}
		}catch(NumberFormatException e)
		{
			System.err.println("ERROR: Cannot convert String into Integer/Float, please choose String datatype");
		}
	}
	
	public RecordRow getSingleRow(int rowIndex) {
		return rows.get(rowIndex);
	}
	public void setSingleRow(RecordRow row,int rowIndex) {
		this.rows.add(rowIndex,row);
	}
	
	public void setMultipleRows(ArrayList<RecordRow> rowlist)
	{
		this.rows = rowlist;
	}
	
	
	public void setMultipleRows(String trows,String rowRegex,String colRegex) {
		String rowslist[] = trows.split(rowRegex);
		this.rows = new ArrayList<RecordRow>();
		
		for(int i =0;i<rowslist.length;i++)
		{
			this.rows.add(new RecordRow(rowslist[i],colRegex));
		}
	}
	
	public ArrayList<RecordRow> getMultipleRows() {
		return this.rows;
	}
	
	public String getMultipleRowsInString()
	{
		String temp ="";
		for(int i=0;i<rows.size();i++)
		{
			if(i==rows.size()-1)
				temp+=rows.get(i).toString();
			else
				temp+=rows.get(i).toString()+MapClass.ROW_DELM;
					
					
		}
				
		return temp;
	}
	
	
	public RecordRow getColNames() {
		return colNames;
	}

	public void setColNames(RecordRow colNames) {
		this.colNames = colNames;
	}

/* Returns the string representation of this multiple Row*/
@Override
public String toString() {
		 
 String temp ="";
	for(int i=0;i<rows.size();i++)
	{
		if(i==rows.size()-1)
			temp+=rows.get(i).toString();
		else
			temp+=rows.get(i).toString()+MapClass.ROW_DELM;
				
				
	}
			
	return temp;
}
	 
	 public void sortRecords(int code)
		{
		 
		 if(rows.size()>0)
		 {
			 Comparator<RecordRow> comparator = null;
			 
			//decide which data type comparison is required
	
				if(MapClass.DATA_TYPE_INDEX==MapClass.DATA_TYPE_INT)
				{
					//integer
					comparator = new Comparator<RecordRow>() {
					    @Override
					    public int compare(RecordRow a, RecordRow b) {
					    	
					    	int res = 0;
					    	
					    	if(a.getCompInteger()>b.getCompInteger())
					    		res = 1;
					    	else if(a.getCompInteger()<b.getCompInteger())
					    		res = -1;
					    	
					        return MapClass.SORT_DIRECTION*res;
					    }
					};
				}
				else if(MapClass.DATA_TYPE_INDEX==MapClass.DATA_TYPE_FLOAT)
				{
					//float
					comparator = new Comparator<RecordRow>() {
					    @Override
					    public int compare(RecordRow a, RecordRow b) {
					    	
					    	int res = 0;
					    	
					    	if(a.getCompFloat()>b.getCompFloat())
					    		res = 1;
					    	else if(a.getCompFloat()<b.getCompFloat())
					    		res = -1;
					    	
					        return MapClass.SORT_DIRECTION*res;
					    }
					};
				}
				else
				{
					//string
					comparator = new Comparator<RecordRow>() {
					    @Override
					    public int compare(RecordRow a, RecordRow b) {
					        return MapClass.SORT_DIRECTION*(a.getCompString().compareTo(b.getCompString()));
					    }
					};
					
				}
				
			 //TODO: assign appropriate algorithm to appropriate cases
			 
			 switch(code)
			 {
			 case SortingController.TIM_SORT:
				//sort the values using comparator
					Collections.sort(rows,comparator);
				 break;
				 
			 case SortingController.INSERTION_SORT:
				  InsertionSort.sort(rows,comparator);
				 break;
				 
			 case SortingController.MERGE_SORT:
				new MergeSort<RecordRow>().sort(rows,comparator);
				 break;
				 
			 case SortingController.QUICK_SORT:
				 new QuickSort<RecordRow>().sort(rows,comparator);
				 break;
				 
			 case SortingController.HEAP_SORT:
				 new HeapSort<RecordRow>().sort(rows,comparator);
				 break;
				 
			 case SortingController.SHELL_SORT:
				 ShellSort.sort(rows,comparator);
				 break;
				 
			 case SortingController.COUNTING_SORT:
				 this.rows=CountingSort.sort(rows);
				 break;
				 
			 case SortingController.RADIX_SORT:
				 RadixSort.sort(rows);
				 break;
				 
			 case SortingController.ADAPTIVE_SORT:
				 InsertionSort.sort(rows,comparator);
				 break;
				 
			 }
		 }
		 else
		 {
			 System.err.println("ERROR: Cannot perform sort!");
		 }
		 
			
		}


	 /**
	  * TODO: UTF has a limit on the size approx 64k bytes, use something that has larger range
	  */
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
		setMultipleRows(arg0.readUTF(),MapClass.ROW_DELM,MapClass.COL_DELM);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(this.toString());
	}
	
}
