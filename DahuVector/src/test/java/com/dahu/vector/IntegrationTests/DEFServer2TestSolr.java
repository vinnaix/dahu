package com.dahu.vector.IntegrationTests;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dahu.TestUtils.*;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 19/10/2018
 * copyright Dahu Ltd 2018
 * <p>
 * Changed by :
 */

public class DEFServer2TestSolr {

    private Process process;
    private String authToken = null;

    @BeforeTest
    public void setupServer(){
        System.out.println("starting DEFServer for Solr on 10106");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ie){

        }
        process = startTestServer("../src/test/resources/testConfig", "VectorDEF.json");
        // get an auth token
        String response = testServerUrl("https://localhost:10106/admin/list","admin","dahudahu");

        // get the authToken from the response
        Pattern pattern = Pattern.compile("\"authToken\": \"(.*?)\"");
        Matcher matcher = pattern.matcher(response);
        if (matcher.find()){
            authToken = matcher.group(1);
        }

    }

    @AfterTest
    public void teardownServer(){
        System.out.println("stopping push API server on 10106");
        stopServer(process);
    }


    @Test
    public void testPushDocToSolr(){
        // test poke pushAPI with good queue only
        System.out.println("[pushAPI] push to queue for Solr");

        String response = testServerPostUrl("https://localhost:10106/api/push/queue1?session="+authToken, true,"../src/test/resources/testConfig/testapifile.txt");
        assertTrueContains("payload\":{\"key\":\"", response);
        System.out.println("[pushAPI] Response from SolrCloudIndexer := " + response );

    }

}
