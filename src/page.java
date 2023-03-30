import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class page implements Serializable {

	private static final long serialVersionUID = 1L;
	int n;
	int NumOfElem;
	String TableName;
	Vector <Record> records;
	Comparable min;
	Comparable max;


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

// 	You are required to use Java’s binary object file (.class) for emulating a page (to avoid
// having you work with file system pages, which is not the scope of this course). A single
// page must be stored as a serialized Vector (java.util.Vector) , because Vectors are thread
// safe). Note that you can save/load any Java object to/from disk by implementing the
// java.io.Serializable interface. You don’t actually need to add any code to your class to
// save it the hard disk


}
