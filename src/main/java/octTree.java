package main.java;

import java.util.ArrayList;

public class octTree {
    Node node;
    String strTableName;
    String[] strarrColName;

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


}