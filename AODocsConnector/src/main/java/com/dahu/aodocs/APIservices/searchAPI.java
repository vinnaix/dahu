package com.dahu.aodocs.APIservices;

import com.dahu.aodocs.AODOCS_CONSTANTS;
import com.dahu.aodocs.types.DahuAODocsDocument;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.exception.BadConfigurationException;
import com.google.auth.oauth2.GoogleCredentials;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 02/08/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 * Wrapper for the AODocs Search API (minimum functionality mapped, as needed to retrieve documents to be indexed)
 *
 */

public class searchAPI  extends DahuAODocsAPIWrapper {


    private String library;
    private String docType;
    private String adminAccount;

    public searchAPI(GoogleCredentials _creds, String _aodAdmin, String _library, String _docType) throws BadConfigurationException,IOException {
        super(_creds);
        this.library = _library;
        this.docType = _docType;
        this.adminAccount = _aodAdmin;
    }


    public searchAPI(String _securityAPICode, String _library, String _docType){
        super(_securityAPICode);
        this.library = _library;
        this.docType = _docType;
    }

    /**
     * Get ALL documents of a given type, present in a given library
     * ALL documents in the library will be included, regardless of when they were inserted into the library
     * A paging mechanism is used to retrieve documents in batches from AODocs, but all are added to the single List
     * to be returned
     * @return List of AODocument objects
     */
    public List<DahuAODocsDocument> getAllResults(){

        List<DahuAODocsDocument> resultList = new ArrayList<>();
        String nextPageToken = getDocumentListPage(null,resultList,0);
        while (null != nextPageToken && nextPageToken.length() > 1){
            nextPageToken = getDocumentListPage(nextPageToken,resultList,0);
        }

        return resultList;
    }

    /**
     * Get first page of documents of a given type, up to 25 documents
     * Documents of any age in the library will be included, regardless of when they were inserted into the library
     * @List<AODocsDocument> a List container - any matching results will be inserted into the container
     * @return String containing the pageToken to supply with any future requests for more pages.
     */
    public String getFirstPageResults(List<DahuAODocsDocument> _resultList){

        return getDocumentListPage(null,_resultList,0);
    }


    /**
     * Request one page of results, for documents with last-mod date after the supplied high-water mark
     * Use @getFirstPageResults to retrieve the first 25 results and to get a page token for page 2
     * @param _pageToken a page token from AODocs representing the next page of results from a cached query
     * @param _resultList a List container - matching AODocuments will be added to this container
     * @param _hwm a Long representing the high-water mark date - documents much have last-mod more recent than this
     * @return string - the page token for the next page of results or null if no more results
     */
    public String getRecentResultsPage(String _pageToken, List<DahuAODocsDocument> _resultList, long _hwm){
        return getDocumentListPage(_pageToken,_resultList,_hwm);
    }

    /**
     * Request one page of results, for documents within the selected library
     * Use @getFirstPageResults to retrieve the first 25 results and to get a page token for page subsequent pages
     * Then keep using the page Tokens to ask for subsequent pages
     * @param _pageToken a page token from AODocs representing the next page of results from a cached query
     * @param _resultList a List container - matching AODocuments will be added to this container
     * @return string - the page token for the next page of results or null if no more results
     */
    public String getResultsPage(String _pageToken, List<DahuAODocsDocument> _resultList){
        return getDocumentListPage(_pageToken,_resultList,0);
    }

    /**
    * Get recent documents of a given type, present in a given library
    * dDocuments in the library will be included in the result list if their last-mod time stamp is more recent than the HWM time stamp
    * A paging mechanism is used to retrieve documents in batches from AODocs, but all are added to the single List
    * to be returned
     * @param _lastCheckedHWM a long representing a time stamp - only documents with a last mod AFTER this time stamp are included in the result List
    * @return List of AODocument objects
     */
    public List<DahuAODocsDocument> getRecentResults(long _lastCheckedHWM){

        List<DahuAODocsDocument> resultList = new ArrayList<>();

        String nextPageToken = getDocumentListPage(null,resultList,_lastCheckedHWM);

        while (null != nextPageToken && nextPageToken.length() > 1){
            nextPageToken = getDocumentListPage(nextPageToken,resultList,_lastCheckedHWM);
        }

        return resultList;


    }


    /**
     * Mechanism for paging through a large result set, adding documents to a List on each iteration
     * and returning the token needed to retrieve the next page each time a page is loaded
     * @param pageToken a Googld search API result list "next page" token to retrieve the next set of results
     * @param _resultList a List of AODocsDocument objects - for each page retrieved, all of its hits are added to this list
     * @param _lastCheckedHWM long - time stamp for when we ran the query. Documents with last-mod time stamp more recent than this are not included in the results
     * @return
     */
    private String getDocumentListPage(String pageToken, List<DahuAODocsDocument> _resultList, long _lastCheckedHWM){

        StringBuilder qString = new StringBuilder();
        String nextPageToken = null;


        String body = "{\n" +
                " \"sort\": {\n" +
                "  \"field\": \"_lastModified\",\n" +
                "  \"direction\": \"DESCENDENT\"\n" +
                " }\n" +
                "}";

        if (null != pageToken){
            qString.append("&pageToken="+pageToken);
        }

        boolean shouldStopNow = false; // True if we get a document from before the last-checked HWM
        try {
            String apiUrl = AODOCS_CONSTANTS.AODOCS_SEARCH_API_URI+ "/" + library + "/list?classId=" + docType + "&pageSize=25";
            if (null != securityCode){
                apiUrl = apiUrl + "&securityCode=" + securityCode;
            }

            String rawResponse = this.getPOSTResponse(apiUrl, body,qString.toString(),adminAccount);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(rawResponse);
            if (null != root && root.isObject()){
                JsonNode docList = root.get("documentList");
                if  (null != docList && docList.isArray()){
                    for (int i = 0; i < ((ArrayNode)docList).size(); i++){
                        JsonNode docNode = ((ArrayNode)docList).get(i);
                        DahuAODocsDocument doc = new DahuAODocsDocument(docNode);
                        // Check for each document if its last mod date is BEFORE the last mod date HWM
                        if (doc.getModificationDate() < _lastCheckedHWM)  {
                            // this doc is from EARLIER than last mod HWM so STOP HERE
                            shouldStopNow = true;
                            i = ((ArrayNode)docList).size()+1;
                        } else {
                            // This result is AFTER last mod HWM so include it in results
                            _resultList.add(doc);

                        }
                    }
                }
            }
            if (! shouldStopNow && null != root.get("pageToken") && null != root.get("pageToken").getTextValue() && root.get("pageToken").getTextValue().length() > 0) {
                nextPageToken = root.get("pageToken").getTextValue();
            }

        } catch (IOException e) {
            logger.warn("IOException in API Request");
            DEFLogManager.LogStackTrace(logger, "AODdocs",e);
        } catch (BadConfigurationException bce){
            logger.warn("BadConfigurationException in API Request : unable to switch to delegated account " + adminAccount);
            DEFLogManager.LogStackTrace(logger, "AODdocs",bce);
        }
        return nextPageToken;
    }



}
