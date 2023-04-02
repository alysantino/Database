package main.java;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    Vector<page> pages;
    String table_name;
    String clustering_key;
    Hashtable<String, String> colNameType;
    Hashtable<String, String> colNameMin;
    Hashtable<String, String> colNameMax;

    public Table(String table_name, String clustering_key, Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws IOException {
        super();
        this.pages = new Vector<page>();
        page page=new page();
        page.setTable(this);
        pages.add(page);
        this.table_name = table_name;
        this.clustering_key = clustering_key;
        this.colNameType = colNameType;
        this.colNameMin = colNameMin;
        this.colNameMax = colNameMax;

        FileWriter fw = new FileWriter("src/main/resources/MetaData.csv", true);
        for (String key : Collections.list(colNameType.keys())) {
            StringBuilder sb = new StringBuilder();
            String metadata_string = this.table_name;
            metadata_string += (',' + key);
            metadata_string += (',' + colNameType.get(key));
            if (key == clustering_key) {
                metadata_string += (',' + "true");
            } else {
                metadata_string += (',' + "false");
            }
            metadata_string += (',' + "null");
            metadata_string += (',' + "null");
            metadata_string += (',' + colNameMin.get(key));
            metadata_string += (',' + colNameMax.get(key));
            metadata_string += "\n";
            sb.append(metadata_string);

            fw.write(sb.toString());
        }
        fw.flush();
        fw.close();

    }
    public Vector<page> getPages() {
        return pages;
    }
    public void addToPages(page page) {
        this.pages.add(page);
    }
    // get and set table name
    public String getTable_name() {
        return table_name;
    }
    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }



    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void main(String[] args) throws IOException {
        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        Hashtable htblColNameMin = new Hashtable();
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "0.0");
        Hashtable htblColNameMax = new Hashtable();
        htblColNameMax.put("id", "100000");
        htblColNameMax.put("name", "ZZZZZZZZZZZ");
        htblColNameMax.put("gpa", "5.0");
        Table t = new Table(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
        System.out.println(0);
    }

}
