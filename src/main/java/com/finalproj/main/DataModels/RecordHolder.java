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
import com.finalproj.main.SortingAlgorithm.InsertionSort;

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
		
		//initialize the actual data
		for(int i =1;i<rowslist.length;i++)
		{
			this.rows.add(new RecordRow(rowslist[i],colRegex));
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
			
		 //TODO: assign appropriate algorithm to appropriate cases
		 
		 switch(code)
		 {
		 case SortingController.TIM_SORT:
			//sort the values using comparator
				Collections.sort(rows,new Comparator<RecordRow>() {
				    @Override
				    public int compare(RecordRow a, RecordRow b) {
				        return a.getSingleCol(MapClass.COL_INDEX).compareTo(b.getSingleCol(MapClass.COL_INDEX));
				    }
				});
			 break;
			 
		 case SortingController.INSERTION_SORT:
			  InsertionSort.sort(rows);
			 break;
			 
		 case SortingController.MERGE_SORT:
			  InsertionSort.sort(rows);
			 break;
			 
		 case SortingController.QUICK_SORT:
			  InsertionSort.sort(rows);
			 break;
			 
		 case SortingController.HEAP_SORT:
			  InsertionSort.sort(rows);
			 break;
			 
		 case SortingController.SHELL_SORT:
			  InsertionSort.sort(rows);
			 break;
			 
		 case SortingController.COUNTING_SORT:
			 InsertionSort.sort(rows);
			 break;
			 
		 case SortingController.RADIX_SORT:
			InsertionSort.sort(rows);
			 break;
			 
		 case SortingController.ADAPTIVE_SORT:
			  InsertionSort.sort(rows);
			 break;
			 
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
