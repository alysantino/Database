package main.java;

import java.io.*;
import java.util.*;

public class DBApp {

    private static final String METADATA_PATH = "Database\\src\\main\\resources\\MetaData.csv";

    public void createTable(String strTableName,
            String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType,
            Hashtable<String, String> htblColNameMin,
            Hashtable<String, String> htblColNameMax)
            throws DBAppException {

        // Check if table already exists
        File metadataFile = new File(METADATA_PATH);
        if (metadataFile.exists()) {
            try (BufferedReader br = new BufferedReader(new FileReader(metadataFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] metadata = line.split(",");
                    if (metadata[0].equals(strTableName)) {
                        throw new DBAppException("Table " + strTableName + " already exists.");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Add new table to metadata
        try (PrintWriter pw = new PrintWriter(new FileOutputStream(new File(METADATA_PATH), true))) {
            for (Map.Entry<String, String> entry : htblColNameType.entrySet()) {
                String columnName = entry.getKey();
                String columnType = entry.getValue();
                String isClusteringKey = columnName.equals(strClusteringKeyColumn) ? "True" : "False";
                String indexName = "N/A";
                String indexType = "N/A";
                String min = htblColNameMin.get(columnName);
                String max = htblColNameMax.get(columnName);
                String metadataLine = strTableName + "," + columnName + "," + columnType + "," +
                        isClusteringKey + "," + indexName + "," + indexType + "," + min + "," + max;
                pw.println(metadataLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBAppException("Error adding table " + strTableName + " to metadata.");
        }
    }

    // Other methods go here...
    
}
