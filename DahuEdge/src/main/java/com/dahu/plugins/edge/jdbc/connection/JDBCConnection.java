package com.dahu.plugins.edge.jdbc.connection;

import com.dahu.core.abstractcomponent.AbstractConnection;
import com.dahu.core.exception.BadConnectionException;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.types.Component;
import com.dahu.plugins.edge.jdbc.JDBC_CONSTANTS;
import org.apache.logging.log4j.Level;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 29/05/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class JDBCConnection extends AbstractConnection {

    private String connectionSpec = null;
    private String driverSpec = null;
    private String username = null;
    private String password = null;

    private String DB_TYPE = null;

    private Connection conn = null;


    public JDBCConnection(Level _l, Component _component){
        super(_l, _component);
    }

    public boolean initialiseMe(){
        this.connectionSpec = properties.get(JDBC_CONSTANTS.CONFIG_CONNECTION_STRING);
        this.driverSpec = properties.get(JDBC_CONSTANTS.CONFIG_DRIVER_CLASSNAME);
        this.username = properties.get(JDBC_CONSTANTS.CONFIG_USERNAME);
        this.password = properties.get(JDBC_CONSTANTS.CONFIG_PASSWORD);

        if (this.driverSpec.endsWith("SQLServerDriver")){
            DB_TYPE = JDBC_CONSTANTS.DB_TYPE_SQLSERVER;
        } else if (this.driverSpec.endsWith("OracleDriver")){
            DB_TYPE = JDBC_CONSTANTS.DB_TYPE_ORACLE;
//        } else if (this.driverSpec.endsWith("mysql")){
//            DB_TYPE = JDBC_CONSTANTS.DB_TYPE_MYSQL;
        } else if (this.driverSpec.startsWith("org.sqlite.JDBC")){
            DB_TYPE = JDBC_CONSTANTS.DB_TYPE_SQLLITE;
        }
        if (null != this.connectionSpec && null != this.driverSpec && null != this.DB_TYPE){
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean connect() throws BadConnectionException {

        try {
            // if we have a connection, close it
            if (null != conn && !conn.isClosed()) {
                conn.commit();
                conn.close();
            }

            // Establish the connection.
            // First make sure the driver class has been loaded by classloader
            if (null != this.driverSpec) {
                try {
                    Class.forName(this.driverSpec);
                } catch (ClassNotFoundException cnfe) {
                    throw new BadConnectionException("JDBC Driver - unable to load class " + this.driverSpec);
                }
            }

            if (null != username && null != password) {
                conn = DriverManager.getConnection(connectionSpec, username, password);
            } else {
                conn = DriverManager.getConnection(connectionSpec);
            }

        } catch (SQLException sqle) {
            throw new BadConnectionException("SQL Error connecting to DB : " + sqle.getLocalizedMessage());
        }

        try {
            if (!conn.isClosed()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException sqle) {
            throw new BadConnectionException("SQL Error connecting to DB : " + sqle.getLocalizedMessage());
        }
    }

    public boolean isConnected(){

        try {
            if (null != conn && ! conn.isClosed()){
                return true;
            } else {
                return false;
            }
        } catch (SQLException sqle){
            return false;
        }

    }

    public boolean close() throws BadConnectionException {
        try {
            if (null != conn && ! conn.isClosed()){
                conn.close();
            }
            if (conn.isClosed()) {
                conn = null;
                return true;
            } else {
                throw new BadConnectionException("Unable to close the JDBC Connection");
            }
        }catch (SQLException sqle){
            DEFLogManager.LogStackTrace(logger, "JDBCConnection",sqle);
            throw new BadConnectionException(sqle.getLocalizedMessage());
        }
    }

        @Override
    public InputStream fetch(String s) {
        return null;
    }

    public Connection getConnection() throws BadConfigurationException, SQLException, BadConnectionException {

        if (null == conn){
            // Establish the connection.
            if (null != this.driverSpec) {
                try {
                    Class.forName(this.driverSpec);
                } catch (ClassNotFoundException cnfe){
                    throw new BadConfigurationException("JDBC Driver - unable to load class " + this.driverSpec);
                }
            }
            if (null != username && null != password) {
                conn = DriverManager.getConnection(connectionSpec, username, password);
            } else {
                conn = DriverManager.getConnection(connectionSpec);
            }
        }

        if (! conn.isClosed()){
            return conn;
        } else {
            throw new BadConnectionException("Unable to open connection to DB");
        }
    }

    public String getDBType(){return this.DB_TYPE;}


}
