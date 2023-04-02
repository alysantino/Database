package main.java;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class page implements Serializable {

	private static final long serialVersionUID = 1L;
	private final int n = 200;
	private int NumOfElem;
	private Table Table;
	private Vector<Record> recordsInPage;
	private int minValueInPage;
	private int maxValueInPage;

	public page() {
		recordsInPage = new Vector<Record>();
		NumOfElem = 0;
		minValueInPage = 0;
		maxValueInPage = 0;
	}

	public int binarySearch(Record htblColNameValue) {
		int low = 0;
		int high = NumOfElem - 1;
		int mid = 0;
		while (low <= high) {
			mid = (low + high) / 2;
			if (recordsInPage.get(mid).compareTo(htblColNameValue) < 0) {
				low = mid + 1;
			} else if (recordsInPage.get(mid).compareTo(htblColNameValue) > 0) {
				high = mid - 1;
			} else {
				return mid;
			}
		}
		return 0;
	}

	// insert record in page sorted and then update min and max and create new page
	// if page is full and shift all records to the right
	public void insert(Record r) {
		int Recordindex = binarySearch(r);
		if (NumOfElem == 0) {
			recordsInPage.add(r);
			NumOfElem++;
			minValueInPage = (int) r.getValues().get("id");
			maxValueInPage = (int) r.getValues().get("id");
		} else
		// if page is full
		if (NumOfElem == n) {
			// get the index of the page in the table
			int Pageindex = Table.getPages().indexOf(this);
			// if the page is the last page in the table
			if (Pageindex == Table.getPages().size() - 1) {
				// create new page
				page newPage = new page();
				newPage.Table = this.Table;
				newPage.setNumOfElem(0);
				// only shift the last record to the new page
				newPage.insert(recordsInPage.get(NumOfElem - 1));
				newPage.setMin((int) newPage.getRecords().get(0).getValues().get("id"));
				newPage.setMax((int) newPage.getRecords().get(newPage.getNumOfElem() - 1).getValues().get("id"));
				// shift all the record that are after the record i want to insert
				int i = NumOfElem - Recordindex - 1;
				while (i > 0) {
					// shift all the records after the i to the right
					recordsInPage.set(Recordindex + i, recordsInPage.get(Recordindex + i - 1));
					i--;
				}
				// insert the record in the right place
				recordsInPage.add(Recordindex, r);
				// insert the newpage in the vector of pages in the table
				Table.getPages().add(newPage);
			} else {
				// check for the first next empty page
				int i = Pageindex + 1;
				int NoOfPagesToShift = 0;
				while (i < Table.getPages().size()) {
					if (Table.getPages().get(i).getNumOfElem() < n) {
						// shift all the record that are after the record i want to insert
						int j = (NoOfPagesToShift * 200 + Table.getPages().get(i).getNumOfElem()) - Recordindex - 1;
						while (j > 0) {
							// shift all the records after the i to the right
							recordsInPage.set(Recordindex + j, recordsInPage.get(Recordindex + j - 1));
							j--;
						}
						// shift all the record that are after the record i want to insert in the page
						int k = NumOfElem - Recordindex - 1;
						while (k > 0) {
							// shift all the records after the i to the right
							recordsInPage.set(Recordindex + k, recordsInPage.get(Recordindex + k - 1));
							k--;
						}
						// insert the record in the right place
						recordsInPage.add(Recordindex, r);
					}
					NoOfPagesToShift++;
				}
			}
		} else if (NumOfElem < n) {
			// shift all the record that are after the record i want to insert
			int i = NumOfElem - Recordindex - 1;
			while (i > 0) {
				// shift all the records after the i to the right
				recordsInPage.set(Recordindex + i, recordsInPage.get(Recordindex + i - 1));
				i--;
			}
			recordsInPage.add(Recordindex, r);
			NumOfElem++;
			if (((Comparable) r.getValues().get("id")).compareTo(minValueInPage) < 0) {
				minValueInPage = (int) recordsInPage.get(0).getValues().get("id");
			}
			if (((Comparable) r.getValues().get("id")).compareTo(maxValueInPage) > 0) {
				maxValueInPage = (int) recordsInPage.get(NumOfElem - 1).getValues().get("id");
			}
		}
	}

	// If all the rows in a page are deleted, then you are required to delete that
	// page. Do not
	// keep around completely empty pages. In the case of insert, if you are trying
	// to insert in a
	// full page, shift one row down to the following page. Do not create a new page
	// unless you
	// are in the last page of the table and that last one was full.

	// getter for min and max

	public Comparable getMin() {
		return minValueInPage;
	}

	public Comparable getMax() {
		return maxValueInPage;
	}

	// setter for min and max
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
		return recordsInPage;
	}

	public void setRecords(Vector<Record> records) {
		this.recordsInPage = records;
	}

	public Table getTable() {
		return Table;
	}

	public void setTable(Table table) {
		Table = table;
	}
}
