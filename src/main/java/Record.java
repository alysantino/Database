package main.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Date;

public class Record extends Hashtable<String, Object> implements Comparable<Record>, Serializable {
    private Hashtable<String, Object> values;
    private String ClusteringKeyType;
    private Object ClusteringKeyValue;
    private String ClusteringKeyName;

    public Record(Hashtable<String, Object> values, String ClusteringKeyType,Table table) {
        super();
        this.values = values;
        this.ClusteringKeyType = ClusteringKeyType;
        try {
            this.ClusteringKeyName = DBApp.getClusteringKeyName(table.getTable_name());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    //update the value of a specific column
    public void updateValue(String columnName, Object value) {
        values.put(columnName, value);
    }

    public Hashtable<String, Object> getValues() {
        return values;
    }

    @Override
    public int compareTo(Record R) {
        if (this.ClusteringKeyType.equals("java.lang.Integer")) {
            return ((Integer) this.getValues().get(this.getClusteringKeyName())).compareTo((Integer) R.getValues().get(R.getClusteringKeyName()));
        } else if (this.ClusteringKeyType.equals("java.lang.Double")) {
            return ((Double) this.getValues().get(this.getClusteringKeyName())).compareTo((Double) R.getValues().get(R.getClusteringKeyName()));
        } else if (this.ClusteringKeyType.equals("java.lang.String")) {
            return ((String) this.getValues().get(this.getClusteringKeyName())).compareTo((String) R.getValues().get(R.getClusteringKeyName()));
        } else if (this.ClusteringKeyType.equals("java.util.Date")) {
            return ((Date) this.getValues().get(this.getClusteringKeyName())).compareTo((Date) R.getValues().get(R.getClusteringKeyName()));
        }
        return 0;
    }

    public Object getClusteringKeyValue(Record R, String strTableName) throws IOException {
        String clusteringKeyName = getClusteringKeyName(strTableName);
        return R.getValues().get(clusteringKeyName);
    }

    private String getClusteringKeyName(String strTableName) throws IOException {
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

    // tostring method to print the record
    public String toString() {
        String s = "";
        for (String key : values.keySet()) {
            s += key + " : " + values.get(key) + " ";
        }
        return s;
    }
    // getters and setters
    public String getClusteringKeyType() {
        return ClusteringKeyType;
    }
     
    public void setClusteringKeyType(String clusteringKeyType) {
        ClusteringKeyType = clusteringKeyType;
    }

    public Object getClusteringKeyValue() {
        return ClusteringKeyValue;
    }

    public void setClusteringKeyValue(Object clusteringKeyValue) {
        ClusteringKeyValue = clusteringKeyValue;
    }

    public String getClusteringKeyName() {
        return ClusteringKeyName;
    }

    public void setClusteringKeyName(String clusteringKeyName) {
        ClusteringKeyName = clusteringKeyName;
    }



}