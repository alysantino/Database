package main.java;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class page implements Serializable {

	private static final long serialVersionUID = 1L;
	private final int n=200;
	private int NumOfElem;
	private Table Table;
	private Vector <Record> records;
	private int minValueInPage;
	private int maxValueInPage;


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
	// insert record in page sorted and then update min and max and create new page if page is full and shift all records to the right
	public void insert(Record r) {
		if (NumOfElem == 0) {
			records.add(r);
			NumOfElem++;
			minValueInPage = (int) r.getValues().get("id");
			maxValueInPage = (int) r.getValues().get("id");
		} else {
			int index = binarySearch(r);
			records.add(index, r);
			NumOfElem++;
			if (((Comparable) r.getValues().get("id")).compareTo(minValueInPage) < 0) {
				minValueInPage =(int) records.get(0).getValues().get("id");
			}
			if (((Comparable) r.getValues().get("id")).compareTo(maxValueInPage) > 0) {
				maxValueInPage = (int) records.get(NumOfElem-1).getValues().get("id");
			}
		}
		if (NumOfElem == n) {
			page p = new page();
			p.Table=this.Table;
			p.setNumOfElem(0);
			p.setRecords(new Vector<Record>());
			for (int i = n / 2; i < n; i++) {
				p.insert(records.get(i));
				records.remove(i);
				NumOfElem--;
			}
			p.setMin((int) p.getRecords().get(0).getValues().get("id"));
			p.setMax((int) p.getRecords().get(p.getNumOfElem() - 1).getValues().get("id"));
			// insert page in the vector of pages in the table
			Table.getPages().add(p);
			NumOfElem++;
		}
	}
	//getter for min and max
	public Comparable getMin() {
		return minValueInPage;
	}
	public Comparable getMax() {
		return maxValueInPage;
	}
	//setter for min and max	
	public void setMin(int min) {
		this.minValueInPage = min;
	}
	public void setMax(int max) {
		this.maxValueInPage = max;
	}
	// getters and setters
	public int getNumOfElem() {
		return NumOfElem;
	}
	public void setNumOfElem(int numOfElem) {
		NumOfElem = numOfElem;
	}
	public String getTableName() {
		return Table.getTable_name();
	}
	public void setTableName(String tableName) {
		Table.setTable_name(tableName);
	}
	public Vector<Record> getRecords() {
		return records;
	}
	public void setRecords(Vector<Record> records) {
		this.records = records;
	}
}
