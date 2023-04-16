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
        System.out.println("Table created successfully");
        tables.put(strTableName, table);
        // print the table names inside the hashtable
        for (String key : tables.keySet()) {
            System.out.println(key);
        }
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
        int id = (int) getClusteringKeyValue(record, strTableName);
        page page = getPage(table, id);

        page.insert(record);
    }

    public void serialize(page page) {
        try {
            FileOutputStream fileOut = new FileOutputStream("src/main/resources/pages.bin");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
            System.out.println("Table serialized and saved to pages.bin");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public page deserialize() {
        page page = null;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/pages.bin");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            page = (page) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("page class not found");
            c.printStackTrace();
            return null;
        }
        return page;

    }
    private String getClusteringKeyType(String strTableName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/MetaData.csv"));
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

    private Object getClusteringKeyValue(Record R, String strTableName) throws IOException {
        String clusteringKeyName = getClusteringKeyName(strTableName);
        return R.getValues().get(clusteringKeyName);
    }

    public Table getTable(String strTableName) {
        return tables.get(strTableName);
    }

    private page getPage(Table table, int id) {
        // get the page from the vector of pages in the table where id is between the min and max of the page
        page page = new page(table);
        for (int i = 0; i < table.getPages().size(); i++) {
            if (id >= (int) table.getPages().get(i).getMin() && id <= (int) table.getPages().get(i).getMax()) {
                page = table.getPages().get(i);
            }
        }
        return page;
    }

    public static void main(String[] args) throws DBAppException, IOException {
        DBApp db = new DBApp();
        db.init();
        // create a new table
        String tableName = "students";
        String clusteringKey = "id";
        Hashtable<String, String> colNameType = new Hashtable<>();
        colNameType.put("id", "java.lang.Integer");
        colNameType.put("name", "java.lang.String");
        colNameType.put("gpa", "java.lang.Double");
        Hashtable htblColNameMin = new Hashtable();
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "0.0");
        Hashtable htblColNameMax = new Hashtable();
        htblColNameMax.put("id", "100000");
        htblColNameMax.put("name", "ZZZZZZZZZZZ");
        htblColNameMax.put("gpa", "5.0");

        db.createTable(tableName, clusteringKey, colNameType, htblColNameMin, htblColNameMax);
        // get the table from the hash table
        Table table = db.getTable(tableName);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 1);
        htblColNameValue.put("name", "Ahmed");
        htblColNameValue.put("gpa", 3.5);
        db.insertIntoTable(tableName, htblColNameValue);
        System.out.println(table.getPages());
        db.serialize(table.getPages().get(0));
        db.serialize(table.getPages().get(0));
        page page1 = db.deserialize();
        System.out.println(page1.getRecords());
   

    }
}
