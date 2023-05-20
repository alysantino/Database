// package main.java;

// import java.io.Serializable;
// import java.util.ArrayList;
// import java.util.Iterator;

// public class octTree implements Serializable{
//     Node node;
//     String strTableName;
//     String[] strarrColName;
//     String indexName;

//     public octTree(Comparable xMin,Comparable xMax, Comparable yMin,Comparable yMax,
//     Comparable zMin,Comparable zMax, String strTableName, String[] strarrColName) throws DBAppException {
//         node = new Node(xMin, xMax, yMin, yMax, zMin, zMax);
//         this.strTableName=strTableName;
//         this.strarrColName=strarrColName;
//         node.setColName(strarrColName);
//         node.setTableName(strTableName);
//         System.out.println("created octree");

//     }

//     public void insert(Record record) throws DBAppException {
//         node.insert(record);
//     }

//     public void delete(Record record) throws DBAppException {
//         node.delete(record);
//     }

//     public Iterator search(SQLTerm[] arrSQLTerms) {
//         return node.search(arrSQLTerms);

//     }


//     //tostring method
//     public String toString(){
//         return node.toString();
//     }

//     //get table name
//     public String getTableName(){
//         return strTableName;
//     }

//     //get column name
//     public String[] getColName(){
//         return strarrColName;
//     }

//     public Node getRoot(){
//         return node;
//     }




// }


package main.java;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;

public class octTree implements Serializable{
    Node node;
    String strTableName;
    String[] strarrColName;
    String indexName;
    private DBApp dbApp;

    public octTree(Comparable xMin,Comparable xMax, Comparable yMin,Comparable yMax,
    Comparable zMin,Comparable zMax, String strTableName, String[] strarrColName) throws DBAppException {
        node = new Node(xMin, xMax, yMin, yMax, zMin, zMax);
        this.strTableName=strTableName;
        this.strarrColName=strarrColName;
        node.setColName(strarrColName);
        node.setTableName(strTableName);
        System.out.println("created octree");

    }

    public void insert(Record record) throws DBAppException {
        node.insert(record);
    }

    public void delete(Hashtable <String,Object> htblColNameValue) throws DBAppException {
        node.delete(htblColNameValue);
    }

    //tostring method
    public String toString(){
        return node.toString();
    }

    //get table name
    public String getTableName(){
        return strTableName;
    }

    //get column name
    public String[] getColName(){
        return strarrColName;
    }

    public Node getRoot(){
        return node;
    }

    public void setDBApp(DBApp dbApp){
        this.dbApp = dbApp;
    }


}