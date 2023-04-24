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
    }

    public void insertIntoTable(String strTableName,
            Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
        String clusteringKeyType = getClusteringKeyType(strTableName);
        if (clusteringKeyType == null) {
            throw new DBAppException("No clustering key found for the table");
        }
        Record record = new Record(htblColNameValue, clusteringKeyType);
        record.setClusteringKeyName(getClusteringKeyName(strTableName));
        record.setClusteringKeyValue(getClusteringKeyValue(record, strTableName));
        Table table = getTable(strTableName);
        if (table == null) {
            throw new DBAppException("No table found with the name " + strTableName);
        }
        Comparable ClusteringKeyValue = (Comparable) getClusteringKeyValue(record, strTableName);
        page page = getPage(table, ClusteringKeyValue);
        if (page.getNumOfElem() > 0)
            deserialize(table, page.getPageindex());
        page.insert(record);
        System.out.println(page.getRecords());
        serialize(page);
    }

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException {
        Table tbl = getTable(strTableName);
        String clusteringKeyType = getClusteringKeyType(strTableName);
        if (clusteringKeyType == null) {
            throw new DBAppException("No clustering key found for the table");
        }
        Record rec = new Record(htblColNameValue, clusteringKeyType);
        rec.setClusteringKeyName(getClusteringKeyName(strTableName));
        rec.setClusteringKeyValue(getClusteringKeyValue(rec, strTableName));
        if (tbl == null) {
            throw new DBAppException("No table found with the name " + strTableName);
        }
        Comparable ClusteringKeyValue = (Comparable) getClusteringKeyValue(rec, strTableName);
        page p = getPage(tbl, ClusteringKeyValue);
        deserialize(tbl, p.getPageindex());
        p.delete(rec);
        System.out.println("Deleted successfully");
        System.out.println(p.getRecords());
        serialize(p);
    }

    public void serialize(page page) {
        try {
            int id = page.getPageindex();
            String fileName = "src/main/resources/pages/" + page.getTableName() + "page_" + id + ".bin";
            FileOutputStream fileOut = new FileOutputStream(fileName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
            System.out.println("Page serialized");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public page deserialize(Table table, int id) {
        // check if the bin file exists
        String fileName = "src/main/resources/pages/" + table.getTable_name() + "page_" + id + ".bin";
        page page = null;
        try {
            FileInputStream fileIn = new FileInputStream(fileName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            page = (page) in.readObject();
            System.out.println("Page deserialized");
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

    private page getPage(Table table, Comparable id) {
        // get the page from the vector of pages in the table where id is between the
        // min and max of the page
        if (table.getPages().size() == 0)
            return new page(table);
        page page = table.getPages().get(0);
        for (int i = 0; i < table.getPages().size(); i++) {
            if (id.compareTo(table.getPages().get(i).getMin()) > 0
                    && id.compareTo(table.getPages().get(i).getMax()) < 0) {
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
        Hashtable<String, Object> record1 = new Hashtable<>();
        Hashtable<String, Object> record2 = new Hashtable<>();
        Hashtable<String, Object> record3 = new Hashtable<>();
        Hashtable<String, Object> record4 = new Hashtable<>();
        Hashtable<String, Object> record5 = new Hashtable<>();
        Hashtable<String, Object> record6 = new Hashtable<>();
        record1.put("id", 1);
        record1.put("name", "Ahmed");
        record1.put("gpa", 3.5);
        record2.put("id", 2);
        record2.put("name", "santino");
        record2.put("gpa", 2.1);
        record3.put("id", 3);
        record3.put("name", "sheko");
        record3.put("gpa", 1.2);
        record4.put("id", 4);
        record4.put("name", "beso");
        record4.put("gpa", 1.2);
        record5.put("id", 5);
        record5.put("name", "zoza");
        record5.put("gpa", 1.2);
        record6.put("id", 6);
        record6.put("name", "mo");
        record6.put("gpa", 1.2);

        db.insertIntoTable(tableName, record3);
        db.insertIntoTable(tableName, record4);
        db.insertIntoTable(tableName, record6);
        db.insertIntoTable(tableName, record2);
        db.insertIntoTable(tableName, record1);
        db.insertIntoTable(tableName, record5);
        db.deleteFromTable(tableName, record4);

    }
}
