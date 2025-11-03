package com.dahu.Edge.unitTests;

import com.dahu.Edge.testUtils;
import com.dahu.core.abstractcomponent.BaseAbstractComponent;
import com.dahu.core.exception.BadConnectionException;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.plugins.PluginFactory;
import com.dahu.def.types.Plugin;
import com.dahu.def.types.Component;
import com.dahu.plugins.edge.jdbc.connection.JDBCConnection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 17/06/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class TestJDBCConnection {

    Logger logger = DEFLogManager.getLogger("TestJDBC", Level.DEBUG);
    JDBCConnection connection = null;
    private static final String JDBCConnectionClass = "com.dahu.plugins.edge.jdbc.connection.JDBCConnection";
    private static final String JARFILE = "DahuEdge.jar";


    @BeforeSuite
    public void createDB(){

        testUtils.setupDB();
    }

    @Test(priority=1)
    public void connectToDB(){

        System.out.println("Starting connect To DB in Unit Test for TestJDBCConnection");

        Component connectionResource = new Component();
        connectionResource.setName("JDBC Test Connection");
        connectionResource.setClassName(JDBCConnectionClass);
        connectionResource.setJar(JARFILE);
        connectionResource.setLogLevel(Level.DEBUG);
        connectionResource.setService("TestConnection");

        Map<String,String> resourceProperties = new HashMap<>();
        resourceProperties.put("driver_class","org.sqlite.JDBC");
        String cwd = System.getProperty("user.dir");
        String DBPATH = null;
        if (cwd.endsWith("target")) {
            DBPATH = "./sqlite/db";
        } else {
            DBPATH = "./target/sqlite/db";
        }


        resourceProperties.put("sql_connection_string","jdbc:sqlite:"+DBPATH+"/tmpdb");

        try {
            connection = (JDBCConnection) PluginFactory.getFactory().getComponent(connectionResource);
        } catch (Exception e){
            System.out.println("Error instantiating new connector : " + e.getLocalizedMessage());
        }

        // OK, we got a Connection - now feed it some config
        if (null != connection){
            ((BaseAbstractComponent) connection).setProperties(resourceProperties);
            ((BaseAbstractComponent)connection).setParent("TestConnection");
            ((BaseAbstractComponent) connection).initialise();
        }

        boolean isConnected = false;
        try {
            isConnected = connection.connect();
        } catch (BadConnectionException bce){
            bce.printStackTrace();
        }
        Assert.assertTrue(isConnected);
    }


    @Test(priority=2)
    public void closeConnectionToDB(){

        System.out.println("Closing connection To DB in Unit Test for TestJDBCConnection");

        boolean isClosed = true;
        if (null != connection && connection.isConnected()){
            try {
                isClosed = connection.close();
            } catch (BadConnectionException bce){
                bce.printStackTrace();
            }
        }
        Assert.assertTrue(isClosed);

    }

}
