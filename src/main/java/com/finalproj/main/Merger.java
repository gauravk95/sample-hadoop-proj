package com.finalproj.main;

import java.util.ArrayList;
import java.util.List;

/*****************************************
 * Merges Multiple Record Holder into one
 * @author Gaurav Kumar
 *****************************************/

//TODO: ABove merger is not working properly...fix it

public class Merger {
	
	public static RecordHolder merge(List<RecordHolder> list){
	
	RecordHolder finalSor = null;
	
		while(list.size()!=1)
		{
			RecordHolder r1 = (RecordHolder) list.get(0);
			RecordHolder r2 = (RecordHolder) list.get(1);
			
			ArrayList<RecordRow> sorted = mergeRecords(r1.getMultipleRows(),r2.getMultipleRows());
			RecordHolder tHolder =new RecordHolder();
			tHolder.setMultipleRows(sorted);
			
			list.add(tHolder);
					
			list.remove(0);
			list.remove(0);
					
		}
		
		finalSor = list.get(0);
		
		return finalSor;
	
	}
	
	private static ArrayList<RecordRow> mergeRecords(ArrayList<RecordRow> ar1,ArrayList<RecordRow> ar2)
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

        
    	return tempAr;
    	
    }

}
