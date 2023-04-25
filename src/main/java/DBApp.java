package main.java;

import java.io.*;
import java.util.*;

public class DBApp {

    private static Hashtable<String, Table> tables;

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
        serialize(page);
    }

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException {
        Table tbl = getTable(strTableName);
        if (tbl == null) {
            throw new DBAppException("No table found with the name " + strTableName);
        }
        String clusteringKeyType = getClusteringKeyType(strTableName);
        if (clusteringKeyType == null) {
            throw new DBAppException("No clustering key found for the table");
        }
        Record rec = new Record(htblColNameValue, clusteringKeyType);
        rec.setClusteringKeyName(getClusteringKeyName(strTableName));
        rec.setClusteringKeyValue(getClusteringKeyValue(rec, strTableName));

        Comparable ClusteringKeyValue = (Comparable) getClusteringKeyValue(rec, strTableName);
        page p = getPage(tbl, ClusteringKeyValue);
        deserialize(tbl, p.getPageindex());
        p.delete(rec);
        System.out.println("Deleted successfully");
        serialize(p);
    }

    public static Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException {
        // check if the array of terms and operators are valid
        if (arrSQLTerms.length != strarrOperators.length + 1) {
            throw new DBAppException("Invalid array of terms and operators");
        }
        Iterator result = null;

        for (int i = 0; i < arrSQLTerms.length - 1; i++) {
            SQLTerm term1 = arrSQLTerms[i];
            SQLTerm term2 = arrSQLTerms[i + 1];
            Iterator iterator1 = SearchInTable(term1.getTableName(), term1.getColumnName(), term1.getOperator(),
                    term1.getValue());
            Iterator iterator2 = SearchInTable(term2.getTableName(), term2.getColumnName(), term2.getOperator(),
                    term2.getValue());
            if (strarrOperators[i].equals("AND"))
                result = intersectIterators(iterator1, iterator2);
            if (strarrOperators[i].equals("OR"))
                result = unionIterators(iterator1, iterator2);
            if (strarrOperators[i].equals("XOR"))
                result = xORIterator(iterator1, iterator2);
        }
        return result;

    }

    private static Iterator intersectIterators(Iterator iterator1, Iterator iterator2) {
        Vector<Record> intersectList = new Vector<Record>();

        while (iterator1.hasNext()) {
            Record tuple1 = (Record) iterator1.next();
            while (iterator2.hasNext()) {
                Record tuple2 = (Record) iterator2.next();
                if (tuple1.compareTo(tuple2) == 0) {
                    intersectList.add(tuple1);
                }
            }
        }
        return intersectList.iterator();
    }

    private static Iterator unionIterators(Iterator iterator1, Iterator iterator2) {
        Vector<Record> unionList = new Vector<Record>();
        // union without duplicates
        while (iterator1.hasNext()) {
            Record tuple1 = (Record) iterator1.next();
            unionList.add(tuple1);
        }
        for (int i = 0; i < unionList.size(); i++) {
            while (iterator2.hasNext()) {
                Record tuple2 = (Record) iterator2.next();
                if (unionList.get(i).compareTo(tuple2) != 0) {
                    unionList.add(tuple2);
                }
            }
        }
        return unionList.iterator();
    }

    private static Iterator xORIterator(Iterator iterator1, Iterator iterator2) {
        Vector<Record> xORList = new Vector<Record>();
        while (iterator1.hasNext()) {
            Record tuple1 = (Record) iterator1.next();
            while (iterator2.hasNext()) {
                Record tuple2 = (Record) iterator2.next();
                if (tuple1.compareTo(tuple2) != 0) {
                    xORList.add(tuple1);
                    xORList.add(tuple2);
                }
            }
        }
        return xORList.iterator();
    }

    private static Iterator SearchInTable(String TableName, String _strColumnName, String _strOperator,
            Object _objValue) {
        Table table = getTable(TableName);
        Vector<Record> matchingRecords = new Vector<Record>();
        // loop on all pages
        for (int i = 0; i < table.getPages().size(); i++) {
            page page = table.getPages().get(i);
            // deserialize the page
            page = deserialize(table, page.getPageindex());
            // loop on all records in the page
            for (int j = 0; j < page.getRecords().size(); j++) {
                Record record = page.getRecords().get(j);
                Object value = record.getValues().get(_strColumnName);
                switch (_strOperator) {
                    case "=":
                        if (value.equals(_objValue)) {
                            matchingRecords.add(record);
                        }
                        break;

                    case ">":
                        if (compareValues(value, _objValue) > 0) {
                            matchingRecords.add(record);
                        }
                        break;

                    case ">=":
                        if (compareValues(value, _objValue) >= 0) {
                            matchingRecords.add(record);
                        }
                        break;

                    case "<":
                        if (compareValues(value, _objValue) < 0) {
                            matchingRecords.add(record);
                        }
                        break;

                    case "<=":
                        if (compareValues(value, _objValue) <= 0) {
                            matchingRecords.add(record);
                        }
                        break;

                    case "!=":
                        if (!value.equals(_objValue)) {
                            matchingRecords.add(record);
                        }
                        break;

                    default:
                        // throw an exception for unsupported operator
                }
            }
            serialize(page);
        }
        return matchingRecords.iterator();
    }

    private static int compareValues(Object value1, Object value2) {
        int x = 0;
        if (value1 instanceof Integer && value2 instanceof Integer) {
            x = ((Integer) value1).compareTo((Integer) value2);
        } else if (value1 instanceof String && value2 instanceof String) {
            x = ((String) value1).compareTo((String) value2);
        } else if (value1 instanceof Date && value2 instanceof Date) {
            x = ((Date) value1).compareTo((Date) value2);
        } else if (value1 instanceof Double && value2 instanceof Double) {
            x = ((Double) value1).compareTo((Double) value2);
        } else {
            // throw an exception for unsupported data type
        }
        return x;
    }

    public static void serialize(page page) {
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

    public static page deserialize(Table table, int id) {
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

    public static Table getTable(String strTableName) {
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
        record4.put("gpa", 2.5);
        record5.put("id", 5);
        record5.put("name", "zoza");
        record5.put("gpa", 1.2);
        record6.put("id", 6);
        record6.put("name", "mo");
        record6.put("gpa", 1.3);

        db.insertIntoTable(tableName, record3);
        db.insertIntoTable(tableName, record4);
        db.insertIntoTable(tableName, record6);
        db.insertIntoTable(tableName, record2);
        db.insertIntoTable(tableName, record1);
        db.insertIntoTable(tableName, record5);
        // db.deleteFromTable(tableName, record4);
        // db.deleteFromTable(tableName, record1);
        // db.deleteFromTable(tableName, record3);
        // db.deleteFromTable(tableName, record6);
        // db.deleteFromTable(tableName, record2);

        SQLTerm[] arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName = "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = "santino";
        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName = "gpa";
        arrSQLTerms[1]._strOperator = ">=";
        arrSQLTerms[1]._objValue = new Double(2.1);
        String[] strarrOperators = new String[1];
        strarrOperators[0] = "OR";
        Iterator resultSet = DBApp.SearchInTable(arrSQLTerms[1]._strTableName, arrSQLTerms[1]._strColumnName,
                arrSQLTerms[1]._strOperator, arrSQLTerms[1]._objValue);
        Iterator finalResult = DBApp.selectFromTable(arrSQLTerms, strarrOperators);

        while (finalResult.hasNext()) {
            int i = 0;
            System.out.println(finalResult.next() + "" + i);
            i++;
        }

    }
}
