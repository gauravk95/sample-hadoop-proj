package com.finalproj.main;

import com.finalproj.main.CustomMapReduceClass.MapClass;

/*********************************************************************************
 * Holds a single Row as an array of columns, consists methods to operate on them
 * @author Gaurav Kumar
 *********************************************************************************/
public class RecordRow{
	
	private String[] cols;
	
	public RecordRow(String cols,String colRegex) {
		this.cols = cols.split(colRegex);
	}
	
	public String[] getMultipleCols() {
		return cols;
	}
	public void setMultipleCols(String[] cols) {
		this.cols = cols;
	}
	
	public String getSingleCol(int colIndex) {
		return cols[colIndex];
	}
	public void setSingleCol(String col,int colIndex) {
		this.cols[colIndex] = col;
	}
	
	
/* Returns the string representation of this Row*/
 @Override
 public String toString() {
     
	 String temp ="";
		for(int i=0;i<cols.length;i++)
		{
			if(i==cols.length-1)
				temp+=cols[i];
			else
				temp+=cols[i]+MapClass.COL_DELM;		
			
		}
		
		return temp;
	 
 }
	
}
