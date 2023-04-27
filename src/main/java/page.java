package main.java;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.KeyStore.Entry;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import javax.security.auth.kerberos.DelegationPermission;

public class page implements Serializable {

	private static final long serialVersionUID = 1L;
	private final int n;
	private static int NumOfElem;
	private static Table Table;
	private Vector<Record> recordsInPage;
	public static Comparable minValueInPage;
	public static Comparable maxValueInPage;
	private int pageID;

	public page(Table t) {
		Table = t;
		Table.getPages().add(this);
		pageID = Table.getPages().indexOf(this);
		recordsInPage = new Vector<Record>();
		NumOfElem = 0;
		n = readMaxNumOfRows();
	}

	public int binarySearch(Record r) {
		int low = 0;
		int high = recordsInPage.size() - 1;
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

	private int readMaxNumOfRows() {
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
		int MaxRows = Integer.parseInt(config.getProperty("MaximumRowsCountinTablePage"));
		return MaxRows;
	}

	// insert record in page sorted and then update min and max and create new page
	// if page is full and shift all records to the right
	public void insert(Record r) {
		int Recordindex = binarySearch(r);
		
		if (recordsInPage.size() == 0) {
			recordsInPage.add(r);
			System.out.println("inserted the first element of the page ");
		} else {
			if (recordsInPage.size() == n) {
				// get the index of the page in the table
				int Pageindex = Table.getPages().indexOf(this);
				// if the page is the last page in the table
				if (Pageindex == Table.getPages().size() - 1) {
					// create new page
					page newPage = new page(Table);	
					if(Recordindex>=n) {
						newPage.getRecords().add(r);
						updatePage(newPage);
						return;
					}
					newPage.getRecords().add(this.recordsInPage.get(n - 1));
					newPage.setPageindex(Table.getPages().size());
					recordsInPage.remove(n - 1);
					recordsInPage.add(Recordindex, r);
					updatePage(this);
				} else {
					// check for the first next empty page
					int i = Pageindex ;
					System.out.println(i + " the page to be inserted in");
					int NoOfPagesToShift = 0;
					while (i < Table.getPages().size()) {
						if (Table.getPages().get(i).getRecords().size() < n) {
							
							break;
						}
						NoOfPagesToShift++;
						i++;
					}
					i=Pageindex;
					System.out.println("NoOfPagesToShift " + NoOfPagesToShift);
					for (int j = 0; j < NoOfPagesToShift; j++) {
						System.out.println(r + " the record to be inserted") ;
						Record lastRecord = Table.getPages().get(i).getRecords().get(n - 1);
						Table.getPages().get(i+1).getRecords().add(0, lastRecord);
						Table.getPages().get(i).getRecords().remove(n - 1);
						i++;
					}
				}
			}
			if (recordsInPage.size() < n && recordsInPage.size() != 0) {
				recordsInPage.add(Recordindex, r);
			}

		}
		updatePage(this);
	}

	public void update(Record r, Hashtable<String, Object> values) throws Exception {
		int location = binarySearch(r);
		Record toBeUptaded = recordsInPage.get(location);
		for (String key : values.keySet()) {
			toBeUptaded.updateValue(key, values.get(key));
		}

	}

	private static String getClusteringKeyName(String strTableName) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/MetaData.csv"));
		String line;
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split(",");
			if (parts[0].equals(strTableName) && parts[3].equals("true")) {
				return parts[1];
			}
		}
		reader.close();
		return null; // no clustering key found for the table
	}

	void updatePage(page p) {
		p.setNumOfElem(p.recordsInPage.size());
		if (p.getNumOfElem() == 0) {
			p = null;
		}
		// update min and max
		String clusteringkey = p.getRecords().get(0).getClusteringKeyName();
		p.setMin((Comparable) recordsInPage.get(0).getValues().get(clusteringkey));
		p.setMax((Comparable) recordsInPage.get(recordsInPage.size() - 1).getValues().get(clusteringkey));
	}

	public void delete(Record r) {
		int Recordindex = binarySearch(r);
		recordsInPage.remove(Recordindex);
		updatePage(this);
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

	public static String getTableName() {
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

	public void setPageindex(int id) {
		pageID = id;
	}

	// tosring method returns the table name and the number of elements in the page
	public String toString() {
		return recordsInPage.toString() ;
	}

}
