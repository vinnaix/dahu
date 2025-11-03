package com.dahu.aodocs.APIservices;

import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.plugins.core.auth.Google.GoogleCredentialsFactory;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import org.apache.logging.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 02/08/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 *
 * Helper Class that wraps calls to the AODocs REST APIs
 *
 * The various AODocs APIs all expose a REST interface, over http, but the actual methods vary between the APIs and API methods.
 * This wrapper class exposes methods for calling to AODocs using GET, POST, PUT returning Strings, and also GET returning a raw HttpResponse object
 *
 * The various Google API client libraries encapsulate security and HTTP calls to the REST API, using an instance of GoogleCredentials, a wrapper for OAuth
 * which in our case comes initially from the service account JSON file. In some cases, that instance needs to be cloned to allow the credentials
 * to delegate to a named user account, typically because the user will be a Google domain admin while a service account should not be.
 * However, the AODocs API doesnt have a simple client layer to hide this stuff, so we need to call it using lower level HTTPRequest classes.
 * Fortunately, handling the security is simple. Taking a GoogleCredentials instance, we can get an AccessToken, and the GoogleCredentials class
 * takes care of any refreshes that need to take place when the token expires. Given an Access Token, we stuff it as a header in requests to AODocs
 * name = Authorization
 * value = "Bearer " + $ACCESSTOKEN as string
 *
 */

public class DahuAODocsAPIWrapper {

    protected Logger logger;

    protected HttpRequestFactory requestFactory = null;
    protected HttpContent content = null;
    protected HttpHeaders headers = new HttpHeaders(); // contains request headers to send to API; some are fixed, others assigned for each request

    // These two security models are mutually exclusive. We are either working with the API code or with OAUuth / GoogleCredentials
    protected GoogleCredentials serviceCredentials = null;
    protected String securityCode = null;

//    protected String AODocsAdminAccount = null;
    private Map<String, String> json = new HashMap<String, String>();  // JSON structure to send with every request  ; default information set in constructor


    /**
     * Constructor for wrapper based on an app security code
     * for Development and Debug only
     * @param _appSecurityCode a Google Project Application Security Code
     */
    public DahuAODocsAPIWrapper(String _appSecurityCode) {
        try {
            logger = DEFLogManager.getLogger("AODocs");
        } catch (BadConfigurationException bce){
            logger = DEFLogManager.getDEFSysLog();
        }

        securityCode = _appSecurityCode;
        json.put("explicitAccessOnly", "false");
        json.put("securityCode", securityCode);
        content = new JsonHttpContent(new JacksonFactory(), json);
        requestFactory = new NetHttpTransport().createRequestFactory();
        headers.set("X-GOOG-API-FORMAT-VERSION", "2");

    }

    /**
     * Constructor for wrapper based on an OAuth authentication model where a user has authenticated to Google
     * and generated a GoogleCredentials instance
     * @param _creds a GoogleCredentials instance created by a successful authentication over OAuth
     * @throws BadConfigurationException thrown if we are unable to create a delegated GoogleCredentials from the one supplied to the constructor
     */
    public DahuAODocsAPIWrapper(GoogleCredentials _creds) throws BadConfigurationException, IOException {


        if (null == _creds){
            throw new BadConfigurationException("GoogleCredentials instance was null - must authenticate first before calling AODocs APIs");
        }

        serviceCredentials = _creds;
        serviceCredentials.refreshIfExpired();

        json.put("explicitAccessOnly", "false");
        content = new JsonHttpContent(new JacksonFactory(), json);

        requestFactory = new NetHttpTransport().createRequestFactory();
        headers.set("X-GOOG-API-FORMAT-VERSION", "2");


    }


    /**
     * Wrapper for an HTTP PUT request to an AODocs REST API
     * @param _url the URL representing an AODocs REST API that supports PUT requests
     * @return the raw response from the AODocs REST API service, parsed as a String
     */
    protected String getPUTResponse(String _url, String _delegatedAccount) throws BadConfigurationException{
        HttpRequest request = null;

        HttpHeaders tempheaders = getSecuredHeaders(_delegatedAccount);

        try {
            request = requestFactory.buildPutRequest(new GenericUrl(_url), content);
            request.setHeaders(tempheaders);
            request.setReadTimeout(60000);
            return request.execute().parseAsString();

        } catch (IOException e) {
            if (null != logger) {
                logger.warn("IOException in PUT Request");
                DEFLogManager.LogStackTrace(logger, "AODdocs", e);
            } else {
                e.printStackTrace();
            }
        }
        return null;
    }


    /**
     * Wrapper for an HTTP GET request to an AODocs REST API
     * The request is submitted and the raw HTTP Response object is returned
     * @param _url URL for an AODocs REST API that supports GET requests
     * @return HttpResponse object returned by AODocs REST API
     */
    protected HttpResponse getGETHttpResponse(String _url, String _delegatedAccount) throws BadConfigurationException {
        HttpRequest request = null;
        HttpHeaders tempheaders = getSecuredHeaders(_delegatedAccount);

        try {

            request = requestFactory.buildGetRequest(new GenericUrl(_url));
            request.setHeaders(tempheaders);
            return request.execute();

        } catch (IOException e) {
            if (null != logger) {
                logger.warn("IOException in PUT Request");
                DEFLogManager.LogStackTrace(logger, "AODdocs", e);
            } else {
                e.printStackTrace();
            }
        }
        return null;
    }


    /**
     * Wrapper for an HTTP GET request to an AODocs REST API
     * @param _url URL for an AODocs REST API that supports GET requests
     * @return String returned by AODocs REST API
     */
    protected String getGETResponse(String _url, String _delegatedAccount) throws BadConfigurationException{
        HttpRequest request = null;
        HttpHeaders tempheaders = getSecuredHeaders(_delegatedAccount);

        try {

            request = requestFactory.buildGetRequest(new GenericUrl(_url));
            request.setHeaders(tempheaders);
            return request.execute().parseAsString();

        } catch (EOFException eofe){
            if (null != logger) {
                logger.warn("EOFException in PUT Request");
                DEFLogManager.LogStackTrace(logger, "AODdocs", eofe);
            } else {
                eofe.printStackTrace();
            }
        } catch (IOException e) {
            if (null != logger) {
                logger.warn("IOException in PUT Request");
                DEFLogManager.LogStackTrace(logger, "AODdocs", e);
            } else {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * Wrapper for an HTTP POST request to an AODocs REST API assuming data sent to the REST API is JSON
     * @param _url URL for an AODocs REST API that supports POST requests
     * @param _body String containing the JSON data to be POSTed to the REST API
     * @param _qString String containing QueryString parameters to be sent to the request API
     * @return String returned by AODocs REST API
     */
    protected String getPOSTResponse(String _url, String _body, String _qString, String _delegatedAccount) throws BadConfigurationException {
        HttpRequest request = null;
        HttpHeaders tempheaders = getSecuredHeaders(_delegatedAccount);

        try {

            String requestBody;
            if (null != _body) {
                requestBody = _body;
            } else {
                requestBody = "{}";
            }

            String finalUrlStr = _url;
            if (null != _qString){
                finalUrlStr = finalUrlStr + "&" + _qString;
            }
            GenericUrl finalUrl = new GenericUrl(finalUrlStr);

            request = requestFactory.buildPostRequest(finalUrl, ByteArrayContent.fromString("application/json", requestBody));
            request.setHeaders(tempheaders);

            request.getHeaders().setContentType("application/json");

            return request.execute().parseAsString();

        } catch (IOException e) {
            if (null != logger) {
                logger.warn("IOException in PUT Request");
                DEFLogManager.LogStackTrace(logger, "AODdocs", e);
            } else {
                e.printStackTrace();
            }
        }
        return null;

    }

    /**
     * To send a request to Google or AODocs API with secure credentials, the request must have an access token sent
     * as a Header. Access tokens can be retrieved from GoogleCredentials instance
     * Credentials can be delegated to a named Admin Account by supplying the account name.
     * If GoogleCredentials can delegate to the supplied name, then access token is stuffed into the Headers and these are returned
     * If there is no account to delegate then an access token from the service account is used instead
     * @param _delegateAccount an account with permission to READ from AODocs Libraries
     * @return HttpHeaders with access token added
     * @throws BadConfigurationException thrown if unable to delegate to the supplied account
     */
    private HttpHeaders getSecuredHeaders(String _delegateAccount) throws BadConfigurationException{

        GoogleCredentials creds = null;
        if (null != _delegateAccount){
            creds = GoogleCredentialsFactory.getDelegatedCredentials(serviceCredentials, _delegateAccount);
        } else {
            // probably not going to work but we can at least try to call the API with the service account
            creds = serviceCredentials;
        }
        HttpHeaders newHeaders = headers.clone();
        try {
            creds.refreshIfExpired();
            AccessToken tok = creds.refreshAccessToken();
            String token = tok.getTokenValue();
            newHeaders.set("Authorization", "Bearer " + token);
        } catch (Exception e){
            e.printStackTrace();
        }
        return newHeaders;

    }

}
