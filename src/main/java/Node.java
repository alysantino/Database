package main.java;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Node {
    ArrayList<Node>children = new ArrayList<Node>();
    ArrayList<Record> points = new ArrayList<Record>();
    Comparable xMin;
    Comparable xMax;
    Comparable yMax;
    Comparable yMin;
    Comparable zMin;
    Comparable zMax;
    String strTableName;
    String[] strarrColName;
    

    public Node(Comparable xMin,Comparable xMax, Comparable yMin,Comparable yMax,
    Comparable zMin,Comparable zMax){
        this.xMin=xMin;
        this.xMax=xMax;
        this.yMin=yMin;
        this.yMax=yMax;
        this.zMin=zMin;
        this.xMax=zMax;
    }

    public boolean isLeaf(){
        return children.size()==0;
    }

    public boolean isFull(){
        return points.size()==readMaxNumOfEntries();
    }

    public void split(){  //GET MID POINT
        children.add(new Node(xMin, getMid(xMin,xMax), yMin, getMid(yMin,yMax), zMin, getMid(zMin,zMax)));
        children.add(new Node(xMin, getMid(xMin,xMax), yMin, getMid(yMin,yMax), getMid(zMin,zMax), zMax));
        children.add(new Node(xMin, getMid(xMin,xMax), getMid(yMin,yMax), yMax, getMid(zMin,zMax), zMax));
        children.add( new Node(xMin, getMid(xMin,xMax), getMid(yMin,yMax), yMax, zMin, getMid(zMin,zMax)));
        children.add(new Node(getMid(xMin,xMax), xMax, yMin, getMid(yMin,yMax), zMin, getMid(zMin,zMax)));
        children.add(new Node(getMid(xMin,xMax), xMax, yMin, getMid(yMin,yMax), getMid(zMin,zMax), zMax));
        children.add(new Node(getMid(xMin,xMax), xMax, getMid(yMin,yMax), yMax, zMin, getMid(zMin,zMax)));
        children.add( new Node(getMid(xMin,xMax), xMax, getMid(yMin,yMax), yMax, getMid(zMin,zMax), zMax));
    }

    public Comparable getMid(Comparable min, Comparable max){
        if(max instanceof Integer){
            return (int)max-(int)min;
        }
        if(max instanceof Double){
            return (double)max-(double)min;
        }
        if(max instanceof Date){
           return ((Date)max).getTime()-((Date)min).getTime();
        }
        if(max instanceof String){
            //loop on the 2 
        }
        return null;
    }


    private void redistribute(Node node) {
        //loop on node points and insert it to the children if it lies in the range of the child
        for(int i=0;i<node.points.size();i++){
            // loop on all the children and insert the point in the child that it lies in its range
            for(int j=0;j<node.children.size();j++){
                if(node.children.get(j).isInRange(node.points.get(i))){
                    node.children.get(j).points.add(node.points.get(i));
                }
            }
        }
        node.points.clear();

    }

    private boolean isInRange(Record record) {
        for(int i=0;i<3;i++){
            if(((Comparable)record.getValues().get(strarrColName[i])).compareTo(this.xMin)>0 && ((Comparable)record.getValues().get(strarrColName[i])).compareTo(this.xMax)<0){
                if(((Comparable)record.getValues().get(strarrColName[i+1])).compareTo(this.yMin)>0 && ((Comparable)record.getValues().get(strarrColName[i+1])).compareTo(this.yMax)<0){
                    if(((Comparable)record.getValues().get(strarrColName[i+2])).compareTo(this.zMin)>0 && ((Comparable)record.getValues().get(strarrColName[i+2])).compareTo(this.zMax)<0){
                        return true;
                    }
                }
            }
        }
        return false;
        
    }


    public void insert(Record record) {
    }
    

    private int readMaxNumOfEntries() {
		Properties config = new Properties();
		FileInputStream inConfig = null;
		try {
			inConfig = new FileInputStream("src/main/resources/DBApp.config");
			config.load(inConfig);
			inConfig.close();
		} catch (IOException e5) {
			// TODO Auto-generated catch block
			e5.printStackTrace();
		}
		int MaxRows = Integer.parseInt(config.getProperty("MaximumEntriesinOctreeNode"));
		return MaxRows;
	}
	
    public void setTableName(String strTableName){
        this.strTableName=strTableName;
    }
    public void setColName(String[] strarrColName){
        this.strarrColName=strarrColName;
    }


}