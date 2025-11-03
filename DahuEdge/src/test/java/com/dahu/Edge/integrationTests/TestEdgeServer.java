package com.dahu.Edge.integrationTests;

import com.dahu.Edge.testUtils;
import org.sqlite.SQLiteException;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.dahu.Edge.testUtils.*;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 03/05/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class TestEdgeServer {

    // The working directory is $PROJECT/target
    // All paths must be relative to ./target

    private Process process;
    private static final String outputFilePath_crawl = "../target/TestQueueOutput.txt";
    private static final String outputFilePath_jdbc = "../target/TestQueueOutput_jdbc.txt";
    private static final String HWMFilePath = "../target/TestJDBC.hwm";

    private String authToken = null;


    @BeforeSuite
    public void setupServer(){

        System.out.println("Starting DB...");
        testUtils.setupDB();

        System.out.println("starting full config server on 10104");

        // start a fully-featured service of DEFServer
        process = startTestServer("../src/test/resources/testConfig","testConfig.json");

        // get an auth token
        String response = testServerUrl("https://localhost:10104/admin/list","admin","dahudahu");

        // get the authToken from the response
        Pattern pattern = Pattern.compile("\"authToken\": \"(.*?)\"");
        Matcher matcher = pattern.matcher(response);
        if (matcher.find()){
            authToken = matcher.group(1);
        }


    }


    @AfterSuite
    public void teardownServer(){
        System.out.println("stopping full config server on 10104");
        stopServer(process);
        tearDownDB();
    }


    // service starts with autostart on the FS crawler and Consumer.
    // All files that are queued for indexeing are written to a temp text file under /target
    //Read the contents and confirm the expected set of files are present

    @Test(priority = 2)
    public void testCrawlOutput(){

        System.out.println("[FS Crawler] Testing files that are queued for INSERT");
        System.out.println("Waiting for Listeners to register before starting the crawl");
        // need to wait for it to crawl the folders and write to the output file
        try {
            Thread.sleep(25000);
        } catch (InterruptedException ioe){
            // do nothing
        }
        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines( Paths.get(outputFilePath_crawl), StandardCharsets.UTF_8))
        {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        assertTrueContains("crawldocs/folder2add/addme.txt",contentBuilder.toString());
        assertTrueContains("crawldocs/folder2add/subfolder2add/addme2.txt",contentBuilder.toString());
        assertTrueDoesNotContain("addme.not",contentBuilder.toString());
        assertTrueDoesNotContain("notme.txt",contentBuilder.toString());
        assertTrueDoesNotContain("folder2exclude",contentBuilder.toString());
        System.out.println("Finished crawling");
    }


    @Test(priority = 3)
    public void testJDBCOutput(){

        System.out.println("[JDBC] Testing JDBC Connector");
        // need to wait for it to crawl the folders and write to the output file
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ioe){
            // do nothing
        }
        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines( Paths.get(outputFilePath_jdbc), StandardCharsets.UTF_8))
        {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        assertTrueContains("101",contentBuilder.toString());
        assertTrueContains("104",contentBuilder.toString());
        assertTrueDoesNotContain("99",contentBuilder.toString());

        StringBuilder contentBuilder2 = new StringBuilder();
        try (Stream<String> stream = Files.lines( Paths.get(HWMFilePath), StandardCharsets.UTF_8))
        {
            stream.forEach(s -> contentBuilder2.append(s).append("\n"));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        assertTrueContains("104",contentBuilder2.toString());
        System.out.println("completed testing of JDBC output");


    }


}
