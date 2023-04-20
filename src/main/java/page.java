package main.java;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class page implements Serializable {

	private static final long serialVersionUID = 1L;
	private final int n = 200;
	private int NumOfElem;
	private Table Table;
	private Vector<Record> recordsInPage;
	private Comparable minValueInPage;
	private Comparable maxValueInPage;
	private int pageID;

	public page(Table t) {
		Table = t;
		Table.getPages().add(this);
		pageID = Table.getPages().indexOf(this);
		recordsInPage = new Vector<Record>();
		NumOfElem = 0;
	}

	public int binarySearch(Record r) {
		int low = 0;
		int high = NumOfElem - 1;
		int mid = 0;
		while (low <= high) {
			mid = (low + high) / 2;
			if (recordsInPage.get(mid).compareTo(r) < 0) {
				low = mid + 1;
			} else if (recordsInPage.get(mid).compareTo(r) > 0) {
				high = mid - 1;
			} else {
				return mid;
			}
		}
		return low;
	}

	// insert record in page sorted and then update min and max and create new page
	// if page is full and shift all records to the right
	public void insert(Record r) {
		int Recordindex = binarySearch(r);
		if (NumOfElem == 0) {
			recordsInPage.add(r);
			System.out.println("inserted the first element of the page ");
		} else {
			// if page is full
			if (NumOfElem == n) {
				// get the index of the page in the table
				int Pageindex = Table.getPages().indexOf(this);
				// if the page is the last page in the table
				if (Pageindex == Table.getPages().size() - 1) {
					// create new page
					page newPage = new page(this.Table);
					// only shift the last record to the new page
					newPage.insert(recordsInPage.get(NumOfElem - 1));
					updatePage(newPage);
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
			}
			if (NumOfElem < n && NumOfElem != 0) {
				recordsInPage.add(Recordindex, r);
			}

		}
		updatePage(this);
	}

	private void updatePage(page p) {
		p.setNumOfElem(p.recordsInPage.size());
		// update min and max
		String clusteringkey = p.getRecords().get(0).getClusteringKeyName();
		p.setMin((Comparable) recordsInPage.get(0).getValues().get(clusteringkey));
		p.setMax((Comparable) recordsInPage.get(NumOfElem - 1).getValues().get(clusteringkey));
	}

	public void delete(Record r) {
		int Recordindex = binarySearch(r);
		// shift all the record that are after the record i want to delete
		int i = NumOfElem - Recordindex - 1;
		while (i > 0) {
			// shift all the records after the i to the left
			recordsInPage.set(Recordindex + i - 1, recordsInPage.get(Recordindex + i));
			i--;
		}
		recordsInPage.remove(NumOfElem - 1);
		NumOfElem--;
		if (NumOfElem == 0) {
			minValueInPage = 0;
			maxValueInPage = 0;
		} else {
			minValueInPage = (int) recordsInPage.get(0).getValues().get("id");
			maxValueInPage = (int) recordsInPage.get(NumOfElem - 1).getValues().get("id");
		}
	}

	// getter for min and max

	public Comparable getMin() {
		return minValueInPage;
	}

	public Comparable getMax() {
		return maxValueInPage;
	}

	// setter for min and max
	public void setMin(Comparable min) {
		this.minValueInPage = min;
	}

	public void setMax(Comparable max) {
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

	public int getPageindex() {
		return pageID;
	}

	// tosring method returns the table name and the number of elements in the page
	public String toString() {
		return "Table name: " + Table.getTable_name() + " Number of elements: " + NumOfElem;
	}

}
