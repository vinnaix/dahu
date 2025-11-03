package com.dahu.plugins.edge.jdbc;

import com.dahu.core.document.DEFDocument;
import com.dahu.core.interfaces.iDocument;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.annotations.DEFAnnotationMT;
import com.dahu.def.config.PluginConfig;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.exception.MQException;
import com.dahu.def.plugins.YieldingService;
import com.dahu.def.types.MQueue;
import com.dahu.def.types.Service;
import org.apache.logging.log4j.Level;

import javax.jms.JMSException;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import static com.dahu.plugins.edge.jdbc.JDBC_CONSTANTS.*;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 23/10/2017
 * copyright Dahu Ltd 2017
 *
 * DEF Service that connects over JDBC to a relational DB, selects a set of documents,
 * and pushes them to a queue for processing/indexing
 *
 * <p>
 * Changed by :
 */


// Must restrict this Instance to single-thread only. We want to run a SQL select and iterate over its results
// Do not want to run multiple SQL selects at the same time iterating over the same rows

@DEFAnnotationMT(isMultiThreaded = "false")
public class JDBCConnectorServiceLegacy extends YieldingService {

    protected String jdbc_driver_className; // JDBC implementation class name
    protected String jdbc_ConnectionString; // connection spec to server, port, service etc
    protected String jdbc_fields; // comma-separated list of columns in the DB to select eg select A,B,C from Table - jdbc_fields = "A,B,C"
    protected String jdbc_table; // DB table or tables - inserted in to SQL select as SELECT BLAH FROM $jdbc_table
    protected String jdbc_where; // WHERE clause for SQL including "WHERE ...."
    protected String jdbc_order_by; // Column or columns for use as order-by or sort order in SQL
    protected String filepath_column; // optional - if one of the DB columns defines a path to a file-sys, this value is set as tmpFilePath on the iDoc
    protected String jdbc_username; // username for DB connection
    protected String jdbc_password; // password for DB Connection
    protected String jdbc_primary_key_list; // comma-separated list of DB columns that form the primary key for items selected
    protected String batch_size_config; // value in config to define number of rows to select from DB in each SQL select execution. If more results exist, the SQL is run again with offset
    protected int max_batch_size = 0; // actual value defining max rows to retrieve - selected from config or default
    protected String hwm_column; // DB column that we use to define the HWM for paging of results

    protected MQueue outputQueue = null;
    protected MQueue.MQueueSession outputQueueSession; // session associated with outputQueue

    protected Set<String> primaryKeyFieldNames = new HashSet<>();

    public JDBCConnectorServiceLegacy(Level _level, Service _instance, int _threadNum) throws JAXBException, MQException, BadConfigurationException,JMSException {
        super(_level,_instance, 1);

        logger.trace("Creating new JDBC Connector");

        // JDBC Connector requires exactly ONE output queue
        if (outputQueues.size() != 1){
            throw new BadConfigurationException("JDBC Connector required ONE output queue");
        }
        outputQueue = (MQueue)outputQueues.toArray()[0];
        outputQueueSession = outputQueue.getSession(serviceName+":"+_threadNum);

        this.jdbc_driver_className = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_DRIVER_CLASSNAME);
        this.jdbc_ConnectionString = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_CONNECTION_STRING);
        this.jdbc_fields = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_SELECT_FIELDS);
        this.jdbc_table = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_SELECT_TABLE);
        this.hwm_column = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_HWM);
        if (hwm_column != null){
            this.jdbc_order_by = hwm_column + " ASC";
        }
        this.jdbc_where = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_SELECT_WHERE_CLAUSE);
        this.filepath_column = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_FILEPATH_NAME);
        this.jdbc_username = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_USERNAME);
        this.jdbc_password = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_PASSWORD);
        this.batch_size_config = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_BATCH_SIZE);
        if (batch_size_config == null){
            max_batch_size = DEFAULT_BATCH_SIZE;
        } else {
            try {
                max_batch_size = Integer.parseInt(batch_size_config);
            } catch (NumberFormatException nfe){
                max_batch_size = DEFAULT_BATCH_SIZE;
            }
        }
        // if config has invalid or pointless batch size, ignore it
        if (max_batch_size < 1){
            max_batch_size = DEFAULT_BATCH_SIZE;
        }

        // split the primary keys into a Set, easier to check column names
        this.jdbc_primary_key_list = PluginConfig.getPluginProperties(_instance.getName()).getPropertyByName(CONFIG_PKS).toLowerCase();
        if (this.jdbc_primary_key_list.indexOf(",") > 0){
            Collections.addAll(primaryKeyFieldNames,this.jdbc_primary_key_list.split(","));
        } else {
            primaryKeyFieldNames.add(this.jdbc_primary_key_list);
        }

    }

    @Override
    protected int doWorkThenYield() throws BadConfigurationException {

        logger.debug("JDBC Connector starting work...");

        int thisIterationRowCounter = 0;
        String hwm_filename = serviceName + ".hwm";
        int hwm = readHWM(hwm_filename);
        int rowcount  = 0;

        // Declare the JDBC objects.
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd;
        String select = null;

        try {

            // Establish the connection.
            if (null != this.jdbc_driver_className) {
                try {
                    Class.forName(this.jdbc_driver_className);
                } catch (ClassNotFoundException cnfe){
                    throw new BadConfigurationException("JDBC Driver - unable to load class " + this.jdbc_driver_className);
                }
            }
            logger.trace("Opening connection to DB.");
            if (null != jdbc_username && null != jdbc_password) {
                con = DriverManager.getConnection(jdbc_ConnectionString, jdbc_username, jdbc_password);
            } else {
                con = DriverManager.getConnection(jdbc_ConnectionString);
            }
            stmt = con.createStatement();
            // build Select statement from config allowing for batches to page across large result set
            select = buildSelectTopNStatement(hwm);

            logger.debug("JDBC Connector Select statement:: " + select);

            rs = stmt.executeQuery(select);
            rsmd = rs.getMetaData();

            // iterate over the result set
            while (rs.next() && rs.getString(1) != null) {

                // first check if we are signalled to stop
                if (super.shouldYield()){
                    logger.debug("Signalled to stop this instance before we finished processing all the rows from SQL");
                    return 0;
                }
                rowcount++;   // counter for how many results we processed in this batch

                // find all the fields that form the primary key - this will be the ID for the iDoc
                StringBuilder primaryKey = new StringBuilder();
                for (String pk : primaryKeyFieldNames){
                    if (rs.getString(pk) != null){
                        primaryKey.append(rs.getString(pk)).append(",");
                    }
                }
                primaryKey.setLength(primaryKey.length()-1);

                // create new iDoc
                iDocument doc = new DEFDocument(primaryKey.toString(),"JDBC://"+serviceName);

                // Currently only support reading String data types from SQL result sets - no blobs
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    try {
                        doc.addField(rsmd.getColumnName(i), rs.getString(i));
                        if (rsmd.getColumnName(i).equalsIgnoreCase(this.filepath_column)) {
                            doc.setTmpFilePath(rs.getString(i));
                        }
                    } catch (SQLException sqle){
                        // failed to read a single field name-value pair, or could not cast as a String.
                        // Carry on with the remaining field values
                        logger.debug("Failed to read value for column from the SQL result set : field name = " + rsmd.getColumnName(i));
                    }
                }


                // We need a HWM to be able to do paging of results. If we didn't find a valid HWM column in the config, don't start the service
                // Currently only valid if the HWM column contains a numeric value
                try {
                    hwm = rs.getInt(hwm_column); // Get first column, which will be the Id column (PK)
                } catch (java.sql.SQLException sqle){
                    throw new BadConfigurationException("JDBCConnector : HWM Column not found : check config for a field to use to sort results by");
                }

                try {
                    outputQueueSession.postDocumentMessage(doc);
                    logger.debug("Added Row to MQ :: First field " + rs.getString(1) + " queue name " + outputQueue.getQueueName()) ;
                } catch (JMSException jmse){
                    logger.warn("Unable to put DEFDocument on queue : JMS error - " + jmse.getLocalizedMessage());
                }

            } // finished reading all the SQL rows
            saveHWM(hwm_filename,hwm);

        } catch (Exception e) {
            logger.error(String.format("service '%s', instance '%s' encountered an error reading from DB: ConnectionString: %s, Statement: %s  :: ERROR %s", service.getName(), serviceName, this.jdbc_ConnectionString, select, e.getLocalizedMessage()));
            e.printStackTrace();
        } finally {
            logger.debug("Closing connection to DB.");
            if (rs != null) try {
                rs.close();
            } catch (Exception e) {
                logger.warn("Exception while closing the connection to DB");
                DEFLogManager.LogStackTrace(logger, "JDBCConnector",e);
            }
            if (stmt != null) try {
                stmt.close();
            } catch (Exception e) {
                // empty - do nothing
            }
            if (con != null) try {
                con.close();
            } catch (Exception e) {
                // empty do nothing
            }
        }

        if (rowcount == 0) {
            return Service.SHUT_ME_DOWN;
        } else if (rowcount < max_batch_size){
            return Service.PAUSE_THEN_RUN;
        } else {
            return Service.RUN_ME_NOW;
        }
}



    protected String buildSelectTopNStatement(int _hwm){

        StringBuilder SQLSelectStatement = new StringBuilder();
        boolean hasWhereClause = false;

        SQLSelectStatement.append("SELECT ");


        // + this.jdbc_fields + " FROM " + this.jdbc_table);

        if (jdbc_driver_className.endsWith("SQLServerDriver")) {
            SQLSelectStatement.append("TOP ").append(max_batch_size);
        }

        SQLSelectStatement.append(" ").append(jdbc_fields).append(" FROM ").append(jdbc_table);


        if (jdbc_where != null && jdbc_where.length() > 0) {
            SQLSelectStatement.append(" ").append(jdbc_where);
            hasWhereClause = true;
        }

        if (_hwm > 0) {
            if (hasWhereClause) {
                SQLSelectStatement.append(" AND ");
            }
            SQLSelectStatement.append(" WHERE Id > ").append(_hwm);
            hasWhereClause = true;
        }

        if (jdbc_driver_className.endsWith("OracleDriver")) {
            if (hasWhereClause) {
                SQLSelectStatement.append(" AND ");
            } else {
                SQLSelectStatement.append(" WHERE");
            }
            SQLSelectStatement.append(" ROWNUM <= ").append( max_batch_size);
        }
        if (jdbc_order_by != null && jdbc_order_by.length() > 0){
            SQLSelectStatement.append(" ORDER BY ").append(this.jdbc_order_by);
        }

        return SQLSelectStatement.toString();
    }

    protected int readHWM(String _filename){

        int hwm = 0;

        Path HWM_FilePath = Paths.get("./" + _filename);
        List<Integer> integers = new ArrayList<>(); // the HWM file should have one line, but this code will read until the end of the file
        // so its a List, although we are only ever interested in the first line
        if (Files.exists(HWM_FilePath)) {
            try {
                Scanner scanner = new Scanner(HWM_FilePath);
                while (scanner.hasNext()) {
                    if (scanner.hasNextInt()) {
                        integers.add(scanner.nextInt());
                    } else {
                        scanner.next();
                    }
                }
                //Finished read data from the HWM file

                if (integers.size() == 1) {
                    hwm = integers.get(0);  // This is our local reference to the HWM value - update it at the end of each loop
                    logger.trace("Reading HWM file - setting HWM => " + hwm);
                }
            } catch (IOException ioe) {
                logger.warn("Failed to open HWM file for " + serviceName + " :: " + ioe.getLocalizedMessage());
            }
        }
        return hwm;

    }

    protected synchronized  void saveHWM(String _filename, int _value){
    /*
        Write the HWM to disk
     */
        List<String> lines = Arrays.asList("" + _value);  // Java NIO wants to write a List of strings to file so give it one, with just one line in it
        Path file = Paths.get("./" + _filename);
        try {
            Files.write(file, lines, Charset.forName("UTF-8"));
        } catch (IOException ioe) {
            logger.warn("Cannot update HWM file, " + _filename + ".hwm" + " :: " + ioe.getLocalizedMessage());
        }
    }

}
