package main.java;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class page implements Serializable {

	private static final long serialVersionUID = 1L;
	private final int n=200;
	private int NumOfElem;
	private String TableName;
	private Vector <Record> records;
	private Comparable min;
	private Comparable max;


	public page() {	
	}
	public int binarySearch(Record htblColNameValue) {
		int low = 0;	
		int high = NumOfElem - 1;
		int mid = 0;
		while (low <= high) {
			mid = (low + high) / 2;
			if (records.get(mid).compareTo(htblColNameValue) < 0) {
				low = mid + 1;
			} else if (records.get(mid).compareTo(htblColNameValue) > 0) {
				high = mid - 1;
			} else {
				return mid;
			}
		}
		return 0;
	}
	//getter for min and max
	public Comparable getMin() {
		return min;
	}
	public Comparable getMax() {
		return max;
	}
	//setter for min and max	
	public void setMin(Comparable min) {
		this.min = min;
	}
	public void setMax(Comparable max) {
		this.max = max;
	}
	// getters and setters
	public int getNumOfElem() {
		return NumOfElem;
	}
	public void setNumOfElem(int numOfElem) {
		NumOfElem = numOfElem;
	}
	public String getTableName() {
		return TableName;
	}
	public void setTableName(String tableName) {
		TableName = tableName;
	}
	public Vector<Record> getRecords() {
		return records;
	}
	public void setRecords(Vector<Record> records) {
		this.records = records;
	}
	



}
