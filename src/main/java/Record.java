package main.java;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Date;

public class Record implements Comparable<Record>, Serializable {
    private Hashtable<String, Object> values;
    private String ClusteringKeyType;

    public Record(Hashtable<String, Object> values, String ClusteringKeyType) {
        super();
        this.values = values;
        this.ClusteringKeyType = ClusteringKeyType;
    }

    public Hashtable<String, Object> getValues() {
        return values;
    }

    @Override
    public int compareTo(Record R) {
        if (this.ClusteringKeyType.equals("java.lang.Integer")) {
            return ((Integer) this.getValues().get("id")).compareTo((Integer) R.getValues().get("id"));
        } else if (this.ClusteringKeyType.equals("java.lang.Double")) {
            return ((Double) this.getValues().get("id")).compareTo((Double) R.getValues().get("id"));
        } else if (this.ClusteringKeyType.equals("java.lang.String")) {
            return ((String) this.getValues().get("id")).compareTo((String) R.getValues().get("id"));
        } else if (this.ClusteringKeyType.equals("java.util.Date")) {
            return ((Date) this.getValues().get("id")).compareTo((Date) R.getValues().get("id"));
        }
        return 0;
    }
    
}