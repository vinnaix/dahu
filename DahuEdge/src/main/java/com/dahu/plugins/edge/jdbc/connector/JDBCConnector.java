package com.dahu.plugins.edge.jdbc.connector;

import com.dahu.core.abstractcomponent.BaseAbstractComponent;
import com.dahu.core.document.DEFDocument;
import com.dahu.core.exception.BadConnectionException;
import com.dahu.core.exception.FailedCacheException;
import com.dahu.core.interfaces.iDocument;
import com.dahu.def.annotations.DEFAnnotationMT;
import com.dahu.def.config.CONFIG_CONSTANTS;
import com.dahu.def.config.PluginConfig;
import com.dahu.def.config.ServerConfig;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.exception.MQException;
import com.dahu.def.plugins.PluginFactory;
import com.dahu.def.plugins.YieldingService;
import com.dahu.def.types.*;
import com.dahu.def.types.Properties;
import com.dahu.plugins.edge.jdbc.JDBC_CONSTANTS;
import com.dahu.plugins.edge.jdbc.connection.JDBCConnection;
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

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 29/05/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

@DEFAnnotationMT(isMultiThreaded = "false")
public class JDBCConnector extends YieldingService {

    JDBCConnection connection = null;

    private static final String CONNECTION = "Connection";

    protected String DB_Type = null;
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
    protected Set<String> primaryKeyFieldNames = new HashSet<>();

    protected MQueue.MQueueSession outputQueueSession = null;


    public JDBCConnector(Level _level, Service _service, int _threadNum) throws JAXBException, MQException, BadConfigurationException,JMSException {
        super(_level, _service, 1);

        // First deal with setting up the Connector
        // find the Connector resource
        Properties props = PluginConfig.getPluginProperties(_service.getName());
        // TODO this is not great as it forced all connectors to have a name of Connection, when this is just a convention.
        Map<String,String> resourceProperties = new HashMap<>();
        for (String propName : props.getNextLevelPropertyNames(CONFIG_CONSTANTS.CONFIGCOMPONENTS+"::"+ CONNECTION)){
            resourceProperties.put(propName, props.getPropertyByName(CONFIG_CONSTANTS.CONFIGCOMPONENTS+"::"+ CONNECTION +"::"+propName));
        }
        if (resourceProperties.size() == 0) { throw new BadConfigurationException("No resource defined for this service"); }
        Component connectionResource = ServerConfig.getComponents().get(CONNECTION);
        if (null == connectionResource){ throw new BadConfigurationException("unable to find a resource named " + CONNECTION + " in service, " + serviceName);}
        connectionResource.setService(serviceName);
        // now load it from jar file
        try {
            connection = (JDBCConnection) PluginFactory.getFactory().getPlugin(connectionResource, Plugin.COMPONENTTYPE);
        } catch (Exception e){
            logger.warn("Error instantiating new connector for " + CONNECTION + " : " + e.getLocalizedMessage());
            throw new BadConfigurationException("Unable to instantiate new Connector for " + CONNECTION);
        }
        // OK, we got a Connection - now feed it some config
        if (null != connection){
            ((BaseAbstractComponent) connection).setProperties(resourceProperties);
            ((BaseAbstractComponent)connection).setParent(serviceName);
            ((BaseAbstractComponent) connection).initialise();
        }
        this.DB_Type = connection.getDBType();

        // Now deal with all the config that we need for the Connector
        this.jdbc_fields = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(JDBC_CONSTANTS.CONFIG_SELECT_FIELDS);
        this.jdbc_table = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(JDBC_CONSTANTS.CONFIG_SELECT_TABLE);
        this.hwm_column = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(JDBC_CONSTANTS.CONFIG_HWM);
        if (hwm_column != null){
            this.jdbc_order_by = hwm_column + " ASC";
        }
        this.jdbc_where = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(JDBC_CONSTANTS.CONFIG_SELECT_WHERE_CLAUSE);
        this.filepath_column = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(JDBC_CONSTANTS.CONFIG_FILEPATH_NAME);
        this.jdbc_username = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(JDBC_CONSTANTS.CONFIG_USERNAME);
        this.jdbc_password = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(JDBC_CONSTANTS.CONFIG_PASSWORD);
        this.batch_size_config = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(JDBC_CONSTANTS.CONFIG_BATCH_SIZE);
        if (batch_size_config == null){
            max_batch_size = JDBC_CONSTANTS.DEFAULT_BATCH_SIZE;
        } else {
            try {
                max_batch_size = Integer.parseInt(batch_size_config);
            } catch (NumberFormatException nfe){
                max_batch_size = JDBC_CONSTANTS.DEFAULT_BATCH_SIZE;
            }
        }
        // if config has invalid or pointless batch size, ignore it
        if (max_batch_size < 1){
            max_batch_size = JDBC_CONSTANTS.DEFAULT_BATCH_SIZE;
        }

        // split the primary keys into a Set, easier to check column names
        this.jdbc_primary_key_list = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(JDBC_CONSTANTS.CONFIG_PKS);
        if (null != this.jdbc_primary_key_list && this.jdbc_primary_key_list.indexOf(",") > 0){
            Collections.addAll(primaryKeyFieldNames,this.jdbc_primary_key_list.split(","));
        } else {
            primaryKeyFieldNames.add(this.jdbc_primary_key_list);
        }

        // Vector output queue - can only be one output queue for a Connector
        outputQueueSession = this.getFirstOutputQueue().getSession(this.serviceName);
    }


    @Override
    protected int doWorkThenYield() throws BadConfigurationException {

        if (null== connection) {
            throw new BadConfigurationException("No connection to database is available");
        }

            String hwm_filename = serviceName + ".hwm";
            int hwm = readHWM(hwm_filename);
            String select = null; // SQL Select statement

            try {
                connection.connect(logger);
                if (connection.isConnected(logger)){
                    Connection conn = connection.getConnection(); // SQL Connection



                    Statement stmt = conn.createStatement();
                    ResultSet rs = null;
                    ResultSetMetaData rsmd;
                    // build Select statement from config allowing for batches to page across large result set
                    select = buildSelectTopNStatement(hwm);
                    logger.debug("JDBC Connector Select statement:: " + select);
                    rs = stmt.executeQuery(select);
                    rsmd = rs.getMetaData();

                    // iterate over the result set
                    // iterate over the result set
                    while (rs.next() && rs.getString(1) != null) {

                        // first check if we are signalled to stop
                        if (super.shouldYield()){
                            logger.debug("Signalled to stop this instance before we finished processing all the rows from SQL");
                            return 0;
                        }

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
                            logger.debug("Added Row to MQ :: First field " + rs.getString(1) + " queue name " + this.getFirstOutputQueue().getQueueName()) ;
                        } catch (JMSException jmse){
                            logger.warn("Unable to put DEFDocument on queue : JMS error - " + jmse.getLocalizedMessage());
                        } catch (FailedCacheException fce){
                            logger.warn("Unable to post document to Vector queue - problem with cache - " + fce.getLocalizedMessage());
                        } catch (MQException mqe){
                            logger.warn("Unable to post document to Vector queue - MQ exception - " + mqe.getLocalizedMessage());
                        }

                    } // finished reading all the SQL rows
                    saveHWM(hwm_filename,hwm);
                }
                connection.close(logger);
            } catch (BadConnectionException bce){
                logger.warn(bce.getLocalizedMessage());
                bce.printStackTrace();
            } catch (SQLException sqle){
                sqle.printStackTrace();
            }
            return Service.SHUT_ME_DOWN;

    }


    protected String buildSelectTopNStatement(int _hwm){

        StringBuilder SQLSelectStatement = new StringBuilder();
        boolean hasWhereClause = false;

        SQLSelectStatement.append("SELECT ");


        // + this.jdbc_fields + " FROM " + this.jdbc_table);

        if (this.DB_Type.equals(JDBC_CONSTANTS.DB_TYPE_SQLSERVER)) {
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

        if (this.DB_Type.equals(JDBC_CONSTANTS.DB_TYPE_ORACLE)) {
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
