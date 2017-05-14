package com.finalproj.main;


import java.awt.*;

import java.awt.event.*;
import java.util.ArrayList;

import javax.swing.*;

import org.jfree.chart.ChartPanel;
import org.jfree.ui.RefineryUtilities;



import com.finalproj.main.CustomMapReduceClass.MapClass;

public class DisplayController {
	
	 private JFrame mainFrame;
	 private JPanel panel ;
	   private JPanel controlPanel;
	   private JComboBox colComboBox;
	   private JComboBox typeComboBox;
	   private ChartPanel barChartPanel;
	   
	   private final String[] dataTypeList = {"String","Integer","Float"};
	   
	   private SortingController sController;

	   public DisplayController(){
		   System.out.println("INFO: Initaializing Display Controller");
		   
		
						
	   }
	   
	   public void initSortController()
	   {
		   sController = new SortingController();
		   
			// TODO Auto-generated method stub
			sController.startSorting();
			
			//update the display
			prepareGUI(sController.getColumnNames(),dataTypeList);
			showJFrame();
			
	   }
	 
	   public void prepareGUI(String[] colNames,String[] typeNames){
		   
		   System.out.println("INFO: Preparing GUI Components");
	      mainFrame = new JFrame("Adaptive Sort");
	      mainFrame.setSize(800,500);
	      
	      panel = new JPanel();
	      panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
	      mainFrame.add(panel);
	      
	      mainFrame.addWindowListener(new WindowAdapter() {
	         public void windowClosing(WindowEvent windowEvent){
	            System.exit(0);
	         }        
	      });    
	     
	      controlPanel = new JPanel();
	      controlPanel.setLayout(new GridLayout(2,3,20,0));

	      //set the dropdown list
	      colComboBox = new JComboBox(colNames);
	      typeComboBox = new JComboBox(typeNames);
	      
	  	System.out.println("INFO: Starting the Visualizer...");
	   }
	   
	   
	   
	public void showJFrame(){
		
	      JButton sortButton = new JButton("Sort Records");
	      sortButton.addActionListener(new ActionListener() {
	         public void actionPerformed(ActionEvent e) {	        	 
	        
	                MapClass.COL_INDEX = colComboBox.getSelectedIndex();;
	                
	                MapClass.DATA_TYPE_INDEX = typeComboBox.getSelectedIndex();
	                
	               sortDataAndUpdateDisplay();
	         }

	      });
	      
	      
	       controlPanel.add(new Label("Select Column:"));
	        controlPanel.add(new Label("Selct DataType:"));
	        controlPanel.add(new Label(" "));
	        controlPanel.add(colComboBox);
	        controlPanel.add(typeComboBox);
	        controlPanel.add(sortButton);
	      
	      //add the control panel to the top
	      panel.add(controlPanel);
	       

	      
	      //add the bar chart to the main frame
		  BarChart chart = new BarChart("Algorithm Execution Time: ",sController.getExecTimes());
				
		  barChartPanel=chart.createBarChart();
		  panel.add(barChartPanel);
				      
	      mainFrame.setVisible(true);  
	   }
	
	
	protected void sortDataAndUpdateDisplay() {
		sController.startSorting();
		
		panel.remove(barChartPanel);
		
		//add the bar chart to the main frame
		  BarChart chart = new BarChart("Algorithm Execution Time: ",sController.getExecTimes());
			
		  barChartPanel = chart.createBarChart();
		  panel.add(barChartPanel);
		  panel.revalidate();
	      panel.repaint();
	}

}
