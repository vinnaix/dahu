package com.dahu.Edge;

import com.dahu.tools.DahuDigestScheme;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.*;
import org.apache.http.ssl.SSLContextBuilder;
import org.sqlite.SQLiteException;
import org.sqlite.core.DB;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.*;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 03/05/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 * Simple tools for starting and stopping a DEF Service so we can test its functionality in situ
 *
 */

public class testUtils {

    private static String DBPATH = null;
    static {
        String cwd = System.getProperty("user.dir");
        if (cwd.endsWith("target")) {
            DBPATH = "./sqlite/db";
        } else {
            DBPATH = "./target/sqlite/db";
        }

    }
    // simple wrapper so we can return a custom failure message
    public static void assertTrueContains(String wanted, String received){
        if (null == received)
            received = "<<NULL>>";
        String failMsg = String.format("Wanted string to contain: '%s', Actual string: '%s'\n", wanted, received);

        assertTrue(received.contains(wanted),failMsg);
    }
    public static void assertTrueDoesNotContain(String wanted, String received){
        if (null == received)
            received = "<<NULL>>";
        String failMsg = String.format("Wanted string to NOT contain: '%s', Actual string: '%s'\n", wanted, received);

        assertFalse(received.contains(wanted),failMsg);
    }


    public static void stopServer(Process _process){
        if (null != _process){
            _process.destroy();
            // give it a few seconds to actually start..
            System.out.println("test DEFServer stopping...");
            try {
                Thread.sleep(5000);
            } catch(Exception e){
                e.printStackTrace();
            }
            System.out.println("test DEFServer stopped");
        }

    }

    public static Process startTestServer(String _configDir, String _configFile){

        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        // note - relies on the fact we have an ant-run task to create a non-versioned jar file
        System.out.println("server startup command is: 'java -jar ./DahuDEFServer.jar -cdir " +  _configDir +  " -c " + _configFile + " -console'");

        ProcessBuilder   ps=new ProcessBuilder("java","-jar", "./DahuDEFServer.jar", "-cdir", _configDir, "-c", _configFile, "-console");
        ps.directory(new File("../target"));

        Process pr;
        try {
            ps.redirectErrorStream(true);
            pr = ps.start();

            // show just a couple of lines of the server startup. Can't show them all as it blocks.
            BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            String line;
            int count = 0;
            while (((line = in.readLine()) != null) && count <4) {
                System.out.println(line);
                count++;
            }

            // give it a few seconds to actually start..
            System.out.println("test DEFServer starting...");
            Thread.sleep(5000);
            System.out.println("test DEFServer started");
            return pr;
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }


    public static String testServerUrl(String _request) {
        return testServerUrl(_request,-1,null,null);
    }
    public static String testServerUrl(String _request,String _username, String _password){
        return testServerUrl(_request,-1,_username,_password);

    }

    public static String testServerUrl(String _request,int _proxyport){
        return testServerUrl(_request,_proxyport,null,null);
    }

    public static String testServerUrl(String _request,int _proxyport,String _username, String _password){
        CloseableHttpClient httpClient;
        CloseableHttpResponse serverResponse;



        URI requestURI;
        try {

            requestURI = new URI(_request);
            // right now, we trust all the https certificates. just makes life a lot easier for not much risk given this is a local-host
            // facility
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, (certificate, authType) -> true).build();
            HttpClientBuilder clientBuilder = HttpClients.custom();
            clientBuilder.setSSLContext(sslContext).setSSLHostnameVerifier(new NoopHostnameVerifier());

            clientBuilder.setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()).build();

            if (_proxyport >-1){
                HttpHost proxy = new HttpHost("localhost", _proxyport);
                clientBuilder.setProxy(proxy);
            }

            httpClient = clientBuilder.build();


            HttpClientContext context = HttpClientContext.create();


            HttpGet httpGet = new HttpGet(requestURI.toString());



            serverResponse = httpClient.execute(httpGet, context);
            if (serverResponse.getStatusLine().getStatusCode() == 401){
                // we got an auth request, so provide the details
                // get the nonce and the realm and make the request again
                Header header = serverResponse.getFirstHeader("WWW-Authenticate");

                String nonce = null;
                String qop = null;
                String realm = null;
                if (null != header){
                    HeaderElement values[] = header.getElements();
                    for (HeaderElement he : values){
                        if (he.getName().equalsIgnoreCase("nonce")){
                            nonce = he.getValue();
                        }
                        if (he.getName().equalsIgnoreCase("qop")){
                            qop = he.getValue();
                        }
                        if (he.getName().equalsIgnoreCase("DahuDigest realm")){
                            realm = he.getValue();
                        }
                        // if (he.getName().equalsIgnoreCase("Digest realm")){
                        //     realm = he.getValue();
                        //}
                    }
                }

                httpClient.close();

                if (_proxyport >-1){
                    HttpHost proxy = new HttpHost("localhost", _proxyport);
                    clientBuilder.setProxy(proxy);
                }
                HttpClientContext context2 = HttpClientContext.create();
                if (null !=_username && null !=_password) {


                    HttpHost targetHost = new HttpHost(requestURI.toURL().getHost(), requestURI.toURL().getPort(), requestURI.toURL().getProtocol());

                    CredentialsProvider credsProvider = new BasicCredentialsProvider();
                    credsProvider.setCredentials(
                            new AuthScope(targetHost.getHostName(), targetHost.getPort()),
                            new UsernamePasswordCredentials(_username, _password));


                    clientBuilder.setDefaultCredentialsProvider(credsProvider);
                    httpClient = clientBuilder.build();

                    AuthCache authCache = new BasicAuthCache();
                    DahuDigestScheme digestAuth = new DahuDigestScheme();
                    digestAuth.overrideParamter("realm", realm);
                    digestAuth.overrideParamter("nonce", nonce);


                    authCache.put(targetHost, digestAuth);

                    context2.setAuthCache(authCache);
                } else {
                    httpClient = clientBuilder.build();
                }


                HttpGet httpGet2 = new HttpGet(requestURI.toString());

                CloseableHttpResponse serverResponse2 = httpClient.execute(httpGet2, context2);

                InputStream input = serverResponse2.getEntity().getContent();
                String content = IOUtils.toString(input, "UTF-8");
                return content;

            } else {
                InputStream input = serverResponse.getEntity().getContent();
                String content = IOUtils.toString(input, "UTF-8");
                return content;
            }

        } catch(Exception e){
            e.printStackTrace();

        }
        return null;
    }


    public static void tearDownDB(){
        File dbdir = new File(DBPATH);

        if (dbdir.exists()){
            dbdir.delete();
        }

    }

    public static void setupDB() {

        File directory = new File(DBPATH);
        if (!directory.exists()) {
            directory.mkdirs();

            String url = "jdbc:sqlite:"+DBPATH+"/tmpdb";

            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            ResultSetMetaData rsmd;

            try {

                conn = DriverManager.getConnection(url);
                stmt = conn.createStatement();
                String sql = "CREATE TABLE TEST_STRINGS " +
                        "(id INTEGER not NULL, " +
                        " FILEPATH VARCHAR(255), " +
                        " TITLE VARCHAR(255), " +
                        " AUTHOR VARCHAR(255), " +
                        " PRIMARY KEY ( id ))";

                try {
                    stmt.executeUpdate(sql);
                } catch (SQLiteException sqle) {
                    // do nothing
                }
                System.out.println("Created table in test database...");
                stmt.execute("DELETE FROM TEST_STRINGS");
                stmt.execute("INSERT INTO TEST_STRINGS VALUES(100,'File1.txt','This is a title','Dahu')");
                stmt.execute("INSERT INTO TEST_STRINGS VALUES(101,'File2.txt','This is a title','Dahu')");
                stmt.execute("INSERT INTO TEST_STRINGS VALUES(102,'File3.txt','This is a title','Dahu')");
                stmt.execute("INSERT INTO TEST_STRINGS VALUES(103,'File4.txt','This is a title','Dahu')");
                stmt.execute("INSERT INTO TEST_STRINGS VALUES(104,'File5.txt','This is a title','Dahu')");
                System.out.println("Inserted data in test database");
            } catch (SQLException sqle) {
                sqle.printStackTrace();
            } finally {
                //finally block used to close resources
                try {
                    if (stmt != null)
                        conn.close();
                } catch (SQLException se) {
                }// do nothing
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException se) {
                    se.printStackTrace();
                }//end finally try
            }
        }
    }

}
