package main.java;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Node {
    ArrayList<Node> children = new ArrayList<Node>();
    ArrayList<Record> points = new ArrayList<Record>();
    Comparable xMin;
    Comparable xMax;
    Comparable yMax;
    Comparable yMin;
    Comparable zMin;
    Comparable zMax;
    String strTableName;
    String[] strarrColName;

    public Node(Comparable xMin, Comparable xMax, Comparable yMin, Comparable yMax,
            Comparable zMin, Comparable zMax) {
        this.xMin = xMin;
        this.xMax = xMax;
        this.yMin = yMin;
        this.yMax = yMax;
        this.zMin = zMin;
        this.zMax = zMax;
    }

    public boolean isLeaf() {
        return children.size() == 0;
    }

    public boolean isFull() {
        return points.size() == readMaxNumOfEntries();
    }

    public void split() { // GET MID POINT
        Node node = new Node(xMin, getMid(xMin, xMax), yMin, getMid(yMin, yMax), zMin, getMid(zMin, zMax));
        node.setColName(this.strarrColName);
        node.setTableName(this.strTableName);
        children.add(node);

        node = new Node(xMin, getMid(xMin, xMax), yMin, getMid(yMin, yMax), getMid(zMin, zMax), zMax);
        node.setColName(this.strarrColName);
        node.setTableName(this.strTableName);
        children.add(node);

        node = new Node(xMin, getMid(xMin, xMax), getMid(yMin, yMax), yMax, getMid(zMin, zMax), zMax);
        node.setColName(this.strarrColName);
        node.setTableName(this.strTableName);
        children.add(node);
        node = new Node(xMin, getMid(xMin, xMax), getMid(yMin, yMax), yMax, zMin, getMid(zMin, zMax));
        node.setColName(this.strarrColName);
        node.setTableName(this.strTableName);
        children.add(node);
        node = new Node(getMid(xMin, xMax), xMax, yMin, getMid(yMin, yMax), zMin, getMid(zMin, zMax));
        node.setColName(this.strarrColName);
        node.setTableName(this.strTableName);
        children.add(node);

        node = new Node(getMid(xMin, xMax), xMax, yMin, getMid(yMin, yMax), getMid(zMin, zMax), zMax);
        node.setColName(this.strarrColName);
        node.setTableName(this.strTableName);
        children.add(node);

        node = new Node(getMid(xMin, xMax), xMax, getMid(yMin, yMax), yMax, zMin, getMid(zMin, zMax));
        node.setColName(this.strarrColName);
        node.setTableName(this.strTableName);
        children.add(node);

        node = new Node(getMid(xMin, xMax), xMax, getMid(yMin, yMax), yMax, getMid(zMin, zMax), zMax);
        node.setColName(this.strarrColName);
        node.setTableName(this.strTableName);
        children.add(node);
    }

    public Comparable getMid(Comparable min, Comparable max) {
        if (max instanceof Integer) {
            return (int) max - (int) min;
        }
        if (max instanceof Double) {
            return (double) max - (double) min;
        }
        if (max instanceof Date) {
            return ((Date) max).getTime() - ((Date) min).getTime();
        }
        if (max instanceof String) {
            return getMiddleString((String) min, (String) max);
        }
        return null;
    }

    private void redistribute(Node node) {
        for (int i = 0; i < node.points.size(); i++) {
            for (int j = 0; j < node.children.size(); j++) {
                System.out.println("in node number " + j);
                System.out.println("point " + node.points.get(i).getValues().get(strarrColName[0]));
                if (node.children.get(j).isInRange(node.points.get(i))) {
                    node.children.get(j).points.add(node.points.get(i));
                    System.out.println("added in node " + node.points.get(i).getValues().get(strarrColName[0]));
                }
            }
        }
        node.points.clear();
    }

    private boolean isInRange(Record record) {
        System.out.println("in is in range " +  this.xMin+ " " + this.xMax+ " " + this.yMin+ " " + this.yMax+ " " + this.zMin+ " " + this.zMax);
        if (((Comparable) record.getValues().get(strarrColName[0])).compareTo(this.xMin) > 0
                && ((Comparable) record.getValues().get(strarrColName[0])).compareTo(this.xMax) < 0) {
            if (((Comparable) record.getValues().get(strarrColName[1])).compareTo(this.yMin) > 0
                    && ((Comparable) record.getValues().get(strarrColName[1])).compareTo(this.yMax) < 0) {
                if (((Comparable) record.getValues().get(strarrColName[2])).compareTo(this.zMin) > 0
                        && ((Comparable) record.getValues().get(strarrColName[2])).compareTo(this.zMax) < 0) {
                    return true;
                }
                return false;
            }
            return false;
        } else
            return false;
    }

    public void insert(Record record) {
        if (points.size() == 0 && children.size() == 0) {
            points.add(record);
            return;
        }

        if (isFull() && isLeaf()) {
            split();
            redistribute(this);
            insert(record);
        } else
            points.add(record);
    }

    private int readMaxNumOfEntries() {
        Properties config = new Properties();
        FileInputStream inConfig = null;
        try {
            inConfig = new FileInputStream("src/main/resources/DBApp.config");
            config.load(inConfig);
            inConfig.close();
        } catch (IOException e5) {
            // TODO Auto-generated catch block
            e5.printStackTrace();
        }
        int MaxRows = Integer.parseInt(config.getProperty("MaximumEntriesinOctreeNode"));
        return MaxRows;
    }

    private static String getMiddleString(String S, String T) {
        // Stores the base 26 digits after addition
        int N;
        if (S.length() > T.length())
            N = T.length();
        else
            N = S.length();
        int[] a1 = new int[N + 1];
        String ans = "";

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int) S.charAt(i) - 97
                    + (int) T.charAt(i) - 97;
        }

        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int) a1[i] / 26;
            a1[i] %= 26;
        }

        for (int i = 0; i <= N; i++) {
            if ((a1[i] & 1) != 0) {
                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }
            a1[i] = (int) a1[i] / 2;
        }

        for (int i = 1; i <= N; i++) {
            ans += (char) (a1[i] + 97);
        }
        return ans;
    }

    public void setTableName(String strTableName) {
        this.strTableName = strTableName;
    }

    public String getTableName() {
        return strTableName;
    }

    public void setColName(String[] strarrColName) {
        this.strarrColName = strarrColName;
    }

    public String toString() {
        String s = "";
        for (int i = 0; i < points.size(); i++) {
            s += points.get(i).toString() + "\n";
        }
        return s;
    }

    public void printNodeValues(){
        System.out.println("xMin: " + xMin + " xMax: " + xMax + " yMin: " + yMin + " yMax: " + yMax + " zMin: " + zMin + " zMax: " + zMax);
    }

}