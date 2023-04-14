package main.java;

import java.io.*;
import java.util.*;

public class DBApp {

    private Hashtable<String, Table> tables;

    public DBApp() {
    }

    public void init() {
        tables = new Hashtable<String, Table>();
    }

    public void createTable(String strTableName,
            String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType,
            Hashtable<String, String> htblColNameMin,
            Hashtable<String, String> htblColNameMax)
            throws DBAppException, IOException {
        Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
    }

    public void insertIntoTable(String strTableName,
            Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
        String clusteringKeyType = getClusteringKeyType(strTableName);
        if (clusteringKeyType == null) {
            throw new DBAppException("No clustering key found for the table");
        }
        Record record = new Record(htblColNameValue, clusteringKeyType);
        Table table = getTable(strTableName);
        if (table == null) {
            throw new DBAppException("No table found with the name " + strTableName);
        }
        int id=(int)getClusteringKeyValue(record, strTableName);
        page page=getPage(table, id);
        page.insert(record);
    }

    private String getClusteringKeyType(String strTableName) throws IOException {
        // read from the metadata file to get the clustering key type of the table then return the clustering key type
        BufferedReader reader = new BufferedReader(new FileReader("MetaData.csv"));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            if (parts[0].equals(strTableName) && parts[3].equals("true")) {
                return parts[2];
            }
        }
        reader.close();
        return null; // no clustering key found for the table
    }
    private Object getClusteringKeyValue(Record R, String strTableName) throws IOException {
        String clusteringKeyType = getClusteringKeyType(strTableName);
        return R.getValues().get(clusteringKeyType);
    }

    public Table getTable(String strTableName) {
        return this.tables.get(strTableName);
    }
    private page getPage(Table table, int id) {
       // get the page from the vector of pages in the table where id is between the min and max of the page
        for(int i=0;i<table.getPages().size();i++) {
            if(id>=table.getPages().get(i).getMin() && id<=table.getPages().get(i).getMax()) {
                return table.getPages().get(i);
            }
        }
        return null;
    }
}
