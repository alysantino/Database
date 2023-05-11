package main.java;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
            throws DBAppException {

        try {
            Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin,
                    htblColNameMax);
            System.out.println("Table created successfully");
            tables.put(strTableName, table);
        } catch (IOException e) {
            throw new DBAppException("table creation failed");
        }

    }

    public boolean tableExists(String TableName) {// new method
        boolean flag = false;
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).getTable_name().equals(TableName)) {
                flag = true;
            }
        }
        return flag;
    }

    public void insertIntoTable(String strTableName,
            Hashtable<String, Object> htblColNameValue) throws DBAppException {

        try {
            String clusteringKeyType = getClusteringKeyType(strTableName);
            Record record = new Record(htblColNameValue, clusteringKeyType, getTable(strTableName));
            record.setClusteringKeyName(getClusteringKeyName(strTableName));
            record.setClusteringKeyValue(getClusteringKeyValue(record, strTableName));

            Table table = getTable(strTableName);
            if (clusteringKeyType == null) {
                throw new DBAppException("No clustering key found for the table");
            }
            if (table == null) {
                throw new DBAppException("No table found with the name " + strTableName);
            }
            for (int i = 0; i < table.getPages().size(); i++) {
                for (int j = 0; j < table.getPages().get(i).getRecords().size(); j++) {
                    if (table.getPages().get(i).getRecords().get(j).getClusteringKeyValue()
                            .equals(record.getClusteringKeyValue())) {
                        throw new DBAppException("Inserting a duplicate clusterting key" + clusteringKeyType);
                    }
                }

            }

            Comparable ClusteringKeyValue = (Comparable) getClusteringKeyValue(record, strTableName);
            page page = getPage(table, ClusteringKeyValue);
            if (page.getNumOfElem() > 0)
                deserialize(table, page.getPageindex());
            if (checkRecordEntries(record, strTableName) == false)
                throw new DBAppException("Record entries are not valid " + record);
            page.insert(record);
            table.updateTable();
            serialize(page);
        } catch (IOException e) {
            throw new DBAppException("Insertion failed");
        }
    }

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        try {
            Table table = getTable(strTableName);
            if (table == null) {
                throw new DBAppException("No table found with the name " + strTableName);
            }
            for (int i = 0; i < table.getPages().size(); i++) {
                for (int j = 0; j < table.getPages().get(i).getRecords().size(); j++) {
                    Record record = table.getPages().get(i).getRecords().get(j);
                    if (record.containsValues(htblColNameValue) == true) {
                        table.getPages().get(i).getRecords().remove(j);
                        if (j != 0) {
                            j--;
                        }
                        if (table.getPages().get(i).getNumOfElem() == 0) {
                            table.getPages().remove(i);
                        }
                    }
                }
                if (table.getPages().get(i).getRecords().size() != 0)
                    table.getPages().get(i).updatePage();
            }
        } catch (IOException e) {
            throw new DBAppException("Deletion failed");
        }
    }

    public void updateTable(String strTableName, String strClusteringKeyValue,
            Hashtable<String, Object> htblColNameValue) throws Throwable {
        Table table = getTable(strTableName);
        if (table == null) {
            throw new DBAppException("No table found with the name " + strTableName);
        }
        String clusteringKeyType = getClusteringKeyType(strTableName);
        if (clusteringKeyType == null) {
            throw new DBAppException("No clustering key found for the table");
        }
        String clusteringKeyName = getClusteringKeyName(strTableName);
        Hashtable<String, Object> clusteringKey = new Hashtable<String, Object>();
        if (getClusteringKeyType(strTableName).equals("java.lang.Integer")) {
            int index = Integer.parseInt(strClusteringKeyValue);
            clusteringKey.put(clusteringKeyName, index);
        } else if (getClusteringKeyType(strTableName).equals("java.lang.Double")) {
            Double index = Double.parseDouble(strClusteringKeyValue);
            clusteringKey.put(clusteringKeyName, index);
        } else if (getClusteringKeyType(strTableName).equals("java.util.Date")) {
            Date index = new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKeyValue);
            clusteringKey.put(clusteringKeyName, index);
        } else {
            clusteringKey.put(clusteringKeyName, strClusteringKeyValue);
        }
        Record record = new Record(clusteringKey, clusteringKeyType, getTable(strTableName));
        page page = getPage(table, (Comparable) getClusteringKeyValue(record, strTableName));
        int index = page.getPageindex();
        // if (checkRecordEntries(record, strTableName) == false)
        //     throw new DBAppException("Record entries are not valid " + record);
        deserialize(table, index);
        page.update(record, htblColNameValue);
        serialize(page);
        table.updateTable();
    }

    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
        if (getTable(strTableName) == null) {
            throw new DBAppException("No table found with the name " + strTableName);
        }
        if (strarrColName.length != 3)
            throw new DBAppException("Invalid number of columns");
        String colName = strarrColName[0];
        String colType = getColType(strTableName, colName);
        Comparable heightMin = getMin(strTableName, colName);
        Comparable heightMax = getMax(strTableName, colName);
        colName = strarrColName[1];
        colType = getColType(strTableName, colName);
        Comparable widthMin = getMin(strTableName, colName);
        Comparable widthMax = getMax(strTableName, colName);
        colName = strarrColName[2];
        colType = getColType(strTableName, colName);
        Comparable depthMin = getMin(strTableName, colName);
        Comparable depthMax = getMax(strTableName, colName);
        octTree index = new octTree(heightMin, heightMax, widthMin, widthMax, depthMin, depthMax, strTableName,
                strarrColName);
        populateIndex(index, strTableName);
        System.out.println(index);
    }


    private Comparable getMax(String tableName, String colName) throws DBAppException {
        Table table = getTable(tableName);
        Comparable max = (Comparable)table.getPages().get(0).getRecords().get(0).getValues().get(colName);
        for (int i = 0; i < table.getPages().size(); i++) {
            page page =deserialize(table, table.getPages().get(i).getPageindex());
            for (int j = 0; j < table.getPages().get(i).getRecords().size(); j++) {
                Record record = page.getRecords().get(j);
                if (((Comparable) record.getValues().get(colName)).compareTo(max) > 0) {
                    max = (Comparable) record.getValues().get(colName);
                }
            }
            serialize(table.getPages().get(i));
        }
        return max;
    }

    private Comparable getMin(String tableName, String colName) throws DBAppException {
        Table table = getTable(tableName);
        Comparable min = (Comparable)table.getPages().get(0).getRecords().get(0).getValues().get(colName);
        for (int i = 0; i < table.getPages().size(); i++) {
            page page =deserialize(table, table.getPages().get(i).getPageindex());
            for (int j = 0; j < page.getRecords().size(); j++) {
                Record record = page.getRecords().get(j);
                if (((Comparable) record.getValues().get(colName)).compareTo(min) < 0) {
                    min = (Comparable) record.getValues().get(colName);
                }
            }
            serialize(table.getPages().get(i));
        }
        return min;
    }

    private void populateIndex(octTree index, String tableName) throws DBAppException {
        Table table = getTable(tableName);
        for (int i = 0; i < table.getPages().size(); i++) {
            for (int j = 0; j < table.getPages().get(i).getRecords().size(); j++) {
                deserialize(table, table.getPages().get(i).getPageindex());
                Record record = table.getPages().get(i).getRecords().get(j);
                index.insert(record);
            }
            serialize(table.getPages().get(i));
        }
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
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

    private Iterator intersectIterators(Iterator iterator1, Iterator iterator2) {
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

    private Iterator unionIterators(Iterator iterator1, Iterator iterator2) {
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

    private Iterator xORIterator(Iterator iterator1, Iterator iterator2) {
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

    private Iterator SearchInTable(String TableName, String _strColumnName, String _strOperator,
            Object _objValue) throws DBAppException {
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
                        throw new DBAppException("Invalid operator");
                }
            }
            serialize(page);
        }
        return matchingRecords.iterator();
    }

    private int compareValues(Object value1, Object value2) throws DBAppException {
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
            throw new DBAppException("Invalid data type");
        }
        return x;
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

    public page deserialize(Table table, int id) throws DBAppException {
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
            throw new DBAppException("Page not found");
        } catch (ClassNotFoundException c) {
            throw new DBAppException("Page not found");
        }
        return page;
    }

    private String getClusteringKeyType(String strTableName) throws DBAppException {
        try {
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
        } catch (IOException e) {
            throw new DBAppException("No clustering key found for the table");
        }
    }

    private String getColType(String strTableName, String colName) throws DBAppException {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/MetaData.csv"));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts[0].equals(strTableName) && parts[1].equals(colName)) {
                    return parts[2];
                }
            }
            reader.close();
            return null; // no clustering key found for the table
        } catch (IOException e) {
            throw new DBAppException("No coloumn type found for the table");
        }
    }

    public static String getClusteringKeyName(String strTableName) throws DBAppException {
        try {
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
        } catch (IOException e) {
            throw new DBAppException("No clustering key found for the table");
        }

    }

    private Object getClusteringKeyValue(Record R, String strTableName) throws DBAppException {
        String clusteringKeyName = getClusteringKeyName(strTableName);
        return R.getValues().get(clusteringKeyName);
    }

    public Table getTable(String strTableName) {
        return tables.get(strTableName);
    }

    private page getPage(Table table, Comparable id) throws DBAppException {
        if (table.getPages().size() == 0)
            return new page(table);
        page page = table.getPages().get(0);
        page lastPage = table.getPages().get(table.getPages().size() - 1);
        if (id.compareTo(page.getMin()) < 0) {
            return page;
        }
        if (id.compareTo(lastPage.getMax()) > 0) {
            return lastPage;
        }
        for (int i = 0; i < table.getPages().size(); i++) {
            page currPage = table.getPages().get(i);
            Comparable min = (Comparable) currPage.getMin();
            Comparable max = (Comparable) currPage.getMax();
            if (currPage.getRecords().size() == currPage.getN() && id.compareTo(max) > 0)
                continue;
            if (currPage.getRecords().size() == 1 && (id.compareTo(min) < 0 || id.compareTo(max) > 0))
                return currPage;
            if (id.compareTo(min) > 0 && id.compareTo(max) < 0) {
                return currPage;
            }
        }
        return page;
    }

    private boolean checkRecordEntries(Record r, String strTableName) throws DBAppException {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/MetaData.csv"));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts[0].equals(strTableName)) {
                    if (parts[2].equals("java.lang.Integer")) {
                        if (!(r.getValues().get(parts[1]) instanceof Integer) || ((Integer) r.getValues().get(parts[1]))
                                .compareTo(Integer.parseInt(parts[6])) < 0
                                || ((Integer) r.getValues().get(parts[1]))
                                        .compareTo(Integer.parseInt(parts[7])) > 0) {
                            reader.close();
                            return false;
                        }
                    } else if (parts[2].equals("java.lang.String")) {
                        if (!(r.getValues().get(parts[1]) instanceof String)) {
                            reader.close();
                            return false;
                        }
                    } else if (parts[2].equals("java.lang.Double")) {

                        if (!(r.getValues().get(parts[1]) instanceof Double)
                                || ((Double) r.getValues().get(parts[1]))
                                        .compareTo(Double.parseDouble(parts[6])) < 0
                                || ((Double) r.getValues().get(parts[1]))
                                        .compareTo(Double.parseDouble(parts[7])) > 0) {
                            reader.close();
                            return false;
                        }
                    } else if (parts[2].equals("java.util.Date")) {
                        try {
                            if (!(r.getValues().get(parts[1]) instanceof Date) || ((Date) r.getValues().get(parts[1]))
                                    .compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(parts[6])) < 0
                                    || ((Date) r.getValues().get(parts[1]))
                                            .compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(parts[7])) > 0) {
                                reader.close();
                                return false;
                            }
                        } catch (ParseException e) {
                            reader.close();
                            throw new DBAppException("Parse Exception");
                        }
                    } else {
                        reader.close();
                        return false;
                    }
                }
            }
            reader.close();
            return true;
        } catch (IOException e) {
            throw new DBAppException("IO Exception");
        }
    }

    public static void main(String[] args) throws Throwable {
        DBApp db = new DBApp();
        db.init();
        String tableName = "students";
        String clusteringKey = "id";
        Hashtable<String, String> colNameType = new Hashtable<>();
        colNameType.put("id", "java.lang.Integer");
        colNameType.put("name", "java.lang.String");
        colNameType.put("gpa", "java.lang.Double");
        Hashtable htblColNameMin = new Hashtable();
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "aaa");
        htblColNameMin.put("gpa", "0.0");
        Hashtable htblColNameMax = new Hashtable();
        htblColNameMax.put("id", "1000");
        htblColNameMax.put("name", "zzz");
        htblColNameMax.put("gpa", "5.0");

        db.createTable(tableName, clusteringKey, colNameType, htblColNameMin, htblColNameMax);

        Table table = db.getTable(tableName);
        Hashtable<String, Object> record1 = new Hashtable<>();
        Hashtable<String, Object> record2 = new Hashtable<>();
        Hashtable<String, Object> record3 = new Hashtable<>();
        Hashtable<String, Object> record4 = new Hashtable<>();
        Hashtable<String, Object> record5 = new Hashtable<>();
        Hashtable<String, Object> record6 = new Hashtable<>();
        Hashtable<String, Object> record7 = new Hashtable<>();
        Hashtable<String, Object> record8 = new Hashtable<>();
        Hashtable<String, Object> record9 = new Hashtable<>();
        Hashtable<String, Object> record10 = new Hashtable<>();
        record1.put("id", 1);
        record1.put("name", "santino");
        record1.put("gpa", 1.2);
        record2.put("id", 2);
        record2.put("name", "zoza");
        record2.put("gpa", 2.1);
        record3.put("id", 3);
        record3.put("name", "beso");
        record3.put("gpa", 1.2);
        record4.put("id", 4);
        record4.put("name", "sheko");
        record4.put("gpa", 1.2);
        record5.put("id", 5);
        record5.put("name", "mo");
        record5.put("gpa", 3.2);
        record6.put("id", 6);
        record6.put("name", "ahmed");
        record6.put("gpa", 2.1);
        // record7.put("id", 7);
        // record7.put("name", "seg");
        // record7.put("gpa", 2.9);
        // record8.put("id", 8);
        // record8.put("name", "cris");
        // record8.put("gpa", 3.6);
        // record9.put("id", 9);
        // record9.put("name", "leo");
        // record9.put("gpa", 1.6);
        // record10.put("id", 9);
        // record10.put("name", "ibra");
        // record10.put("gpa", 1.7);
  
        db.insertIntoTable(tableName, record4);
        db.insertIntoTable(tableName, record3);
        db.insertIntoTable(tableName, record1);
        // db.insertIntoTable(tableName, record5);
        // db.insertIntoTable(tableName, record6);
        db.insertIntoTable(tableName, record2);

        Hashtable<String, Object> delete = new Hashtable<>();
        delete.put("name", "beso");
        // db.deleteFromTable(tableName, delete);

        Hashtable<String, Object> values = new Hashtable<>();
        values.put("gpa", new Double(1.5));
        String strTableName = "students";
        String id = "1";

        db.updateTable(strTableName, id, values);

        System.out.println("Table " + tableName + " records:");
        for (int i = 0; i < table.getPages().size(); i++) {
            page page = table.getPages().get(i);
            for (int j = 0; j < page.getRecords().size(); j++) {
                Record record = page.getRecords().get(j);
                System.out.println(record.getValues());
            }
        }


        String[] strarrColNames = new String[3];
        strarrColNames[0] = "id";
        strarrColNames[1] = "gpa";
        strarrColNames[2] = "name";
        db.createIndex("students", strarrColNames);
        System.out.println("Index created");
        
       
       
        SQLTerm[] arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName = "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = "mo";
        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName = "gpa";
        arrSQLTerms[1]._strOperator = ">";
        arrSQLTerms[1]._objValue = new Double(2.1);
        String[] strarrOperators = new String[1];
        strarrOperators[0] = "OR";
        // Iterator resultSet = db.SearchInTable(arrSQLTerms[1]._strTableName,
        // arrSQLTerms[1]._strColumnName,
        // arrSQLTerms[1]._strOperator, arrSQLTerms[1]._objValue);
        // Iterator finalResult = db.selectFromTable(arrSQLTerms, strarrOperators);

        // while (finalResult.hasNext()) {
        // int i = 0;
        // System.out.println(finalResult.next());
        // i++;
        // }
    }
}
