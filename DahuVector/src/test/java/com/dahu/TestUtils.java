package com.dahu;

import com.dahu.tools.DahuDigestScheme;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.*;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 02/04/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class TestUtils {


    public static void stopServer(Process _process){
        if (null != _process){
            _process.destroyForcibly();
            // give it a few seconds to actually start..
            System.out.println("test DEFServer stopping...");
            try {
                Thread.sleep(5000);
            } catch(Exception e){
                e.printStackTrace();
            }
            if (null != _process && _process.isAlive()) {
                System.out.println("unable to stop test DEFServer - pid " + _process.toString());
            } else {
                System.out.println("test DEFServer stopped");
            }
        }

    }

    public static Process startTestServer(String _configDir, String _configFile){

        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        // note - relies on the fact we have an ant-run task to create a non-versioned jar file
        System.out.println("server startup command is: 'java -jar ./DahuDEFServer.jar -cdir " +  _configDir +  " -c " + _configFile + "'");

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

    public static String testServerPostUrl(String _request, Boolean _process,String _filepath){

        CloseableHttpClient client;
        HttpPost httpPost = new HttpPost(_request);
        URI requestURI;
        try {


            requestURI = new URI(_request);
            // right now, we trust all the https certificates. just makes life a lot easier for not much risk given this is a local-host
            // facility
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, (certificate, authType) -> true).build();
            HttpClientBuilder clientBuilder = HttpClients.custom();
            clientBuilder.setSSLContext(sslContext).setSSLHostnameVerifier(new NoopHostnameVerifier());

            clientBuilder.setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()).build();

            if(_process) {
                httpPost.addHeader("COMPLETIONMODE","PROCESS");
            }
            client = clientBuilder.build();



            HttpEntity entity = MultipartEntityBuilder
                    .create()
                    .addBinaryBody("upload_file", new File(_filepath), ContentType.create("application/octet-stream"), "filename")
                    .build();

            httpPost.setEntity(entity);
            CloseableHttpResponse response = client.execute(httpPost);


            HttpEntity responseEntity = response.getEntity();
            String responseString = null;
            try {
                responseString = EntityUtils.toString(responseEntity, "UTF-8");
            } catch (Exception e ){
                e.printStackTrace();
            }

            return responseString;

        } catch (Exception e){
            System.out.println("big ole problem posting data. " + e );
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


    public static void waitWithMsg(int _ms, String _message){
        try {
            System.out.println(String.format("waiting for %d miliseconds. %s",_ms,_message));
            Thread.sleep(_ms); //  timeout plus one second
        } catch (Exception e){}
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


}
