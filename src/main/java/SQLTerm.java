package main.java;

public class SQLTerm {
    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

    public SQLTerm(String tableName, String columnName, String operator, Object value) {
        this._strTableName = tableName;
        this._strColumnName = columnName;
        this._strOperator = operator;
        this._objValue = value;
    }

    public SQLTerm() {
    }

    public String getTableName() {
        return _strTableName;
    }

    public String getColumnName() {
        return _strColumnName;
    }

    public String getOperator() {
        return _strOperator;
    }

    public Object getValue() {
        return _objValue;
    }
}
