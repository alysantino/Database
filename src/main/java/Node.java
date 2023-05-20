// package main.java;

// import java.io.FileInputStream;
// import java.io.IOException;
// import java.util.ArrayList;
// import java.util.Properties;
// import java.io.*;
// import java.text.ParseException;
// import java.text.SimpleDateFormat;
// import java.util.*;

// public class Node implements Serializable{
//     ArrayList<Node> children = new ArrayList<Node>();
//     ArrayList<Record> points = new ArrayList<Record>();
//     Comparable xMin;
//     Comparable xMax;
//     Comparable yMax;
//     Comparable yMin;
//     Comparable zMin;
//     Comparable zMax;
//     String strTableName;
//     String[] strarrColName;

//     public Node(Comparable xMin, Comparable xMax, Comparable yMin, Comparable yMax,
//             Comparable zMin, Comparable zMax) {
//         this.xMin = xMin;
//         this.xMax = xMax;
//         this.yMin = yMin;
//         this.yMax = yMax;
//         this.zMin = zMin;
//         this.zMax = zMax;
//     }

//     public void insert(Record record) {
//         if (points.size() == 0 && children.size() == 0) {
//             this.points.add(record);
//             return;
//         }
//         if (isFull() && isLeaf()) {
//             this.split();
//             System.out.println("split");
//             this.printNodeChildren();
//             redistribute(this);
//             this.printNodeChildren();
//             Node newnode = getNode(this.children, record);
//             newnode.insert(record);
//             this.printNodeChildren();
//             return;
//         }
//         if (!isFull() && isLeaf()) {
//             this.points.add(record);
//             return;
//         }
//         if (!isLeaf()) {
//             Node node = getNode(this.children, record);
//             node.insert(record);
//             return;
//         }
//     }

//     public ArrayList<Node> search(Record record) {
//         ArrayList<Node> nodes = new ArrayList<Node>();
//         if (isLeaf()) {
//             for (Record r : points) {
//                 if (r.equals(record)) {
//                     nodes.add(this);
//                 }
//             }
//         } else {
//             for (Node node : children) {
//                 if (node.isInRange(record)) {
//                     nodes.addAll(node.search(record));
//                 }
//             }
//         }
//         return nodes;
//     }

//     public void delete(Record record) {
//         ArrayList<Node> nodes = search(record);
//         for (Node node : nodes) {
//             node.deleteFromNode(record);
//         }
//     }

//     public void deleteFromNode(Record record){
//         if (isLeaf()) {
//             for (Record r : points) {
//                 if (r.equals(record)) {
//                     points.remove(r);
//                     return;
//                 }
//             }
//         } else {
//             for (Node node : children) {
//                 if (node.isInRange(record)) {
//                     node.deleteFromNode(record);
//                     return;
//                 }
//             }
//         }
//     }

//     public Iterator<Record> search(SQLTerm[] SQLTerm) {
//         ArrayList<Record> records = new ArrayList<Record>();
//         //colNumber 0 is x 1 is y 2 is z
//     }

//     private Node getNode(ArrayList<Node> children, Record record) {
//         for (Node node : children) {
//             if (node.isInRange(record)) {
//                 return node;
//             }
//         }
//         return null;
//     }

//     public boolean isLeaf() {
//         return children.size() == 0;
//     }

//     public boolean isFull() {
//         return points.size() == readMaxNumOfEntries();
//     }

//     public void split() { // GET MID POINT
//         Node node = new Node(xMin, getMid(xMin, xMax), yMin, getMid(yMin, yMax), zMin, getMid(zMin, zMax));
//         node.setColName(this.strarrColName);
//         node.setTableName(this.strTableName);
//         children.add(node);

//         node = new Node(xMin, getMid(xMin, xMax), yMin, getMid(yMin, yMax), getMid(zMin, zMax), zMax);
//         node.setColName(this.strarrColName);
//         node.setTableName(this.strTableName);
//         children.add(node);

//         node = new Node(xMin, getMid(xMin, xMax), getMid(yMin, yMax), yMax, getMid(zMin, zMax), zMax);
//         node.setColName(this.strarrColName);
//         node.setTableName(this.strTableName);
//         children.add(node);

//         node = new Node(xMin, getMid(xMin, xMax), getMid(yMin, yMax), yMax, zMin, getMid(zMin, zMax));
//         node.setColName(this.strarrColName);
//         node.setTableName(this.strTableName);
//         children.add(node);

//         node = new Node(getMid(xMin, xMax), xMax, yMin, getMid(yMin, yMax), zMin, getMid(zMin, zMax));
//         node.setColName(this.strarrColName);
//         node.setTableName(this.strTableName);
//         children.add(node);

//         node = new Node(getMid(xMin, xMax), xMax, yMin, getMid(yMin, yMax), getMid(zMin, zMax), zMax);
//         node.setColName(this.strarrColName);
//         node.setTableName(this.strTableName);
//         children.add(node);

//         node = new Node(getMid(xMin, xMax), xMax, getMid(yMin, yMax), yMax, zMin, getMid(zMin, zMax));
//         node.setColName(this.strarrColName);
//         node.setTableName(this.strTableName);
//         children.add(node);

//         node = new Node(getMid(xMin, xMax), xMax, getMid(yMin, yMax), yMax, getMid(zMin, zMax), zMax);
//         node.setColName(this.strarrColName);
//         node.setTableName(this.strTableName);
//         children.add(node);
//     }

//     public Comparable getMid(Comparable min, Comparable max) {
//         if (max instanceof Integer) {
//             return (int) min + ((int) max - (int) min) / 2;
//         }
//         if (max instanceof Double) {
//             return (double) min + ((double) max - (double) min) / 2;
//         }
//         if (max instanceof Date) {
//             return ((Date) min).getTime() + (((Date) max).getTime() - ((Date) min).getTime()) / 2;
//         }
//         if (max instanceof String) {
//             return getMiddleString((String) min, (String) max);
//         }
//         return null;
//     }

//     private void redistribute(Node node) {
//         for (int i = 0; i < node.points.size(); i++) {
//             System.out.println("the point is " + node.points.get(i).getValues());
//             for (int j = 0; j < node.children.size(); j++) {
//                 if (node.children.get(j).isInRange(node.points.get(i))) {
//                     node.children.get(j).points.add(node.points.get(i));
//                     System.out.println("added in node " + node.points.get(i).getValues().get(strarrColName[0]));
//                     break;
//                 }
//             }
//         }
//         node.points.clear();
//     }

//     private boolean isInRange(Record record) {
//         this.printNodeRanges();
//         if (((Comparable) record.getValues().get(strarrColName[0])).compareTo(this.xMin) >= 0
//                 && ((Comparable) record.getValues().get(strarrColName[0])).compareTo(this.xMax) <= 0) {
//             if (((Comparable) record.getValues().get(strarrColName[1])).compareTo(this.yMin) >= 0
//                     && ((Comparable) record.getValues().get(strarrColName[1])).compareTo(this.yMax) <= 0) {
//                 if (((Comparable) record.getValues().get(strarrColName[2])).compareTo(this.zMin) >= 0
//                         && ((Comparable) record.getValues().get(strarrColName[2])).compareTo(this.zMax) <= 0) {
//                     System.out.println("in range");
//                     return true;
//                 }
//                 return false;
//             }
//             return false;
//         } else
//             return false;
//     }

//     private int readMaxNumOfEntries() {
//         Properties config = new Properties();
//         FileInputStream inConfig = null;
//         try {
//             inConfig = new FileInputStream("src/main/resources/DBApp.config");
//             config.load(inConfig);
//             inConfig.close();
//         } catch (IOException e5) {
//             // TODO Auto-generated catch block
//             e5.printStackTrace();
//         }
//         int MaxRows = Integer.parseInt(config.getProperty("MaximumEntriesinOctreeNode"));
//         return MaxRows;
//     }

//     private static String getMiddleString(String S, String T) {
//         // Stores the base 26 digits after addition
//         int N;
//         if (S.length() > T.length())
//             N = T.length();
//         else
//             N = S.length();
//         int[] a1 = new int[N + 1];
//         String ans = "";

//         for (int i = 0; i < N; i++) {
//             a1[i + 1] = (int) S.charAt(i) - 97
//                     + (int) T.charAt(i) - 97;
//         }

//         for (int i = N; i >= 1; i--) {
//             a1[i - 1] += (int) a1[i] / 26;
//             a1[i] %= 26;
//         }

//         for (int i = 0; i <= N; i++) {
//             if ((a1[i] & 1) != 0) {
//                 if (i + 1 <= N) {
//                     a1[i + 1] += 26;
//                 }
//             }
//             a1[i] = (int) a1[i] / 2;
//         }

//         for (int i = 1; i <= N; i++) {
//             ans += (char) (a1[i] + 97);
//         }
//         return ans;
//     }

//     public void setTableName(String strTableName) {
//         this.strTableName = strTableName;
//     }

//     public String getTableName() {
//         return strTableName;
//     }

//     public void setColName(String[] strarrColName) {
//         this.strarrColName = strarrColName;
//     }

//     public String toString() {
//         String s = "";
//         for (int i = 0; i < points.size(); i++) {
//             s += points.get(i).toString() + "\n";
//         }
//         return s;
//     }

//     public void printNodeValues() {
//         System.out.println(this.children);
//     }

//     public void printNodeRanges() {
//         System.out.println("xMin: " + xMin + " xMax: " + xMax + " yMin: " + yMin + " yMax: " + yMax + " zMin: " + zMin
//                 + " zMax: " + zMax);
//     }

//     public void printNodeChildren() {
//         System.out.println(this.children);
//     }
// }

package main.java;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Node implements Serializable {
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
    private DBApp db;

    public Node(Comparable xMin, Comparable xMax, Comparable yMin, Comparable yMax,
            Comparable zMin, Comparable zMax) {
        this.xMin = xMin;
        this.xMax = xMax;
        this.yMin = yMin;
        this.yMax = yMax;
        this.zMin = zMin;
        this.zMax = zMax;
    }

    public void insert(Record record) {
        if (points.size() == 0 && children.size() == 0) {
            this.points.add(record);
            return;
        }
        if (isFull() && isLeaf()) {
            this.split();
            System.out.println("split");
            this.printNodeChildren();
            redistribute(this);
            this.printNodeChildren();
            Node newnode = getNode(this.children, record);
            newnode.insert(record);
            this.printNodeChildren();
            return;
        }
        if (!isFull() && isLeaf()) {
            this.points.add(record);
            return;
        }
        if (!isLeaf()) {
            Node node = getNode(this.children, record);
            node.insert(record);
            return;
        }
    }

    private Node getNode(ArrayList<Node> children, Record record) {
        for (Node node : children) {
            if (node.isInRange(record)) {
                return node;
            }
        }
        return null;
    }

    // search for all nodes containing the htblColNameValue
    public ArrayList<Node> search(Hashtable<String, Object> htblColNameValue) {
        ArrayList<Node> nodes = new ArrayList<Node>();
        if (this.contains(htblColNameValue)) {
            nodes.add(this);
        }
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).contains(htblColNameValue)) {
                nodes.add(children.get(i));
            }
        }
        return nodes;
    }

    public void delete(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        ArrayList<Node> nodes = new ArrayList<Node>();
        for (Node node : this.children) {
            node.search(htblColNameValue);
            node.deleteFromNode(htblColNameValue);
        }
    }

    public void deleteFromNode(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        int recIndex;
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).contains(htblColNameValue)) {
                for (int j = 0; j < children.get(i).points.size(); j++) {
                    if (children.get(i).points.get(j).getValues().equals(htblColNameValue)) {
                        children.get(i).points.remove(j);
                        return;
                    }
                }
                return;
            }
        }
        // throw new DBAppException("Record not found");
    }

    // check if a node contains a htblColNameValue
    public boolean contains(Hashtable<String, Object> htblColNameValue) {
        for (int i = 0; i < points.size(); i++) {
            System.out.println(points.get(i).getValues());
            System.out.println(htblColNameValue);
            System.out.println(containsHashtable(htblColNameValue,points.get(i)));
            if (containsHashtable(htblColNameValue,points.get(i))) {
                System.out.println("found");
                return true;
            }
        }
        return false;
    }

    public static boolean containsHashtable(Hashtable<String, Object> searchHashtable, Hashtable<String, Object> targetHashtable) {
        for (String key : searchHashtable.keySet()) {
            if (!targetHashtable.containsKey(key) || !targetHashtable.get(key).equals(searchHashtable.get(key))) {
                return false;
            }
        }
        return true;
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
            return (int) min + ((int) max - (int) min) / 2;
        }
        if (max instanceof Double) {
            return (double) min + ((double) max - (double) min) / 2;
        }
        if (max instanceof Date) {
            return ((Date) min).getTime() + (((Date) max).getTime() - ((Date) min).getTime()) / 2;
        }
        if (max instanceof String) {
            return getMiddleString((String) min, (String) max);
        }
        return null;
    }

    private void redistribute(Node node) {
        for (int i = 0; i < node.points.size(); i++) {
            System.out.println("the point is " + node.points.get(i).getValues());
            for (int j = 0; j < node.children.size(); j++) {
                if (node.children.get(j).isInRange(node.points.get(i))) {
                    node.children.get(j).points.add(node.points.get(i));
                    System.out.println("added in node " + node.points.get(i).getValues().get(strarrColName[0]));
                    break;
                }
            }
        }
        node.points.clear();
    }

    private boolean isInRange(Record record) {
        this.printNodeRanges();
        if (((Comparable) record.getValues().get(strarrColName[0])).compareTo(this.xMin) >= 0
                && ((Comparable) record.getValues().get(strarrColName[0])).compareTo(this.xMax) <= 0) {
            if (((Comparable) record.getValues().get(strarrColName[1])).compareTo(this.yMin) >= 0
                    && ((Comparable) record.getValues().get(strarrColName[1])).compareTo(this.yMax) <= 0) {
                if (((Comparable) record.getValues().get(strarrColName[2])).compareTo(this.zMin) >= 0
                        && ((Comparable) record.getValues().get(strarrColName[2])).compareTo(this.zMax) <= 0) {
                    System.out.println("in range");
                    return true;
                }
                return false;
            }
            return false;
        } else
            return false;
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

    public void printNodeValues() {
        System.out.println(this.children);
    }

    public void printNodeRanges() {
        System.out.println("xMin: " + xMin + " xMax: " + xMax + " yMin: " + yMin + " yMax: " + yMax + " zMin: " + zMin
                + " zMax: " + zMax);
    }

    public void printNodeChildren() {
        System.out.println(this.children);
    }

    public void setDBApp(DBApp dbApp) {
        this.db = dbApp;
    }

    // main method to test split and getmid methods

}