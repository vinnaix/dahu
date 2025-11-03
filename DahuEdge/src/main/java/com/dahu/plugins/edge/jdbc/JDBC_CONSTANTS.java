package com.dahu.plugins.edge.jdbc;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 13/03/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 * CONFIG DEFINITIONS for JDBC Connections
 *
 */

public class JDBC_CONSTANTS {


    // JDBC related


    public static final String CONFIG_DRIVER_CLASSNAME = "driver_class";
    public static final String CONFIG_CONNECTION_STRING = "sql_connection_string";
    public static final String CONFIG_SELECT_FIELDS = "select_fields";
    public static final String CONFIG_SELECT_TABLE = "select_table";
    public static final String CONFIG_SELECT_WHERE_CLAUSE = "where_clause";
    public static final String CONFIG_SELECT_ORDER_BY = "order_by";
    public static final String CONFIG_HWM = "hwm_column";
    public static final String CONFIG_USERNAME = "username";
    public static final String CONFIG_PASSWORD = "password";
    public static final String CONFIG_FILEPATH_NAME = "filepath_column";
    public static final String CONFIG_PKS = "primary_key_column_list";
    public static final String CONFIG_BATCH_SIZE = "batch_size";
    public static final int DEFAULT_BATCH_SIZE = 1000;

    public static final String DB_TYPE_ORACLE = "Oracle";
    public static final String DB_TYPE_SQLSERVER = "SQLServer";
    public static final String DB_TYPE_MYSQL = "MySQL";
    public static final String DB_TYPE_SQLLITE = "SQLLite";


}
