package main.java;

import java.io.*;
import java.util.*;

public class DBApp {

    // private static final String METADATA_PATH =
    // "Database\\src\\main\\resources\\MetaData.csv";
    public DBApp() {
    }

    public void init() {
        File file = new File("src/main/resources/MetaData.csv");
    }

    public void createTable(String strTableName,
            String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType,
            Hashtable<String, String> htblColNameMin,
            Hashtable<String, String> htblColNameMax)
            throws DBAppException, IOException {
                Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax); 
    }
}
