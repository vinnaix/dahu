package com.dahu.aodocs.APIservices;

import com.dahu.aodocs.AODOCS_CONSTANTS;
import com.dahu.aodocs.types.AODComment;
import com.dahu.aodocs.types.DahuAODocsDocument;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.exception.BadConfigurationException;
import com.google.auth.oauth2.GoogleCredentials;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import java.io.IOException;

import static com.dahu.aodocs.AODOCS_CONSTANTS.AODOCS_DOCID_LIMIT;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 07/10/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class documentIdAPI extends DahuAODocsAPIWrapper {

    private String adminAccount;

//GET https://ao-docs.appspot.com/_ah/api/document/v1beta1/RR6yW2Kz51twvu1FNo/comments?domain=dahu.co.uk

    public documentIdAPI(GoogleCredentials _cred,String _aodAdmin) throws BadConfigurationException, IOException {
        super(_cred);
        adminAccount = _aodAdmin;
    }

    public documentIdAPI(String _securityAPICode){
        super(_securityAPICode);
    }


    /**
     * Find all comments attached to a document.
     * For each comment, it is added to the AODocsDocument as a new AODocsComment
     * @param _doc an AODocs Document. Any comments are appended to this document object
     */
    public void readAndSetComments(DahuAODocsDocument _doc){

        String nextPageToken = getCommentsPage(_doc, null);

        while (null != nextPageToken){
            nextPageToken = getCommentsPage(_doc, null);
        }
    }



    private String getCommentsPage(DahuAODocsDocument _doc, String _nextPageToken) {



        String requestUrl = AODOCS_CONSTANTS.AODOCS_DOCID_API_URI + "/" + _doc.getDocId() + "/comments?domain=" + _doc.getDomain() + AODOCS_DOCID_LIMIT;
        if (null != securityCode) {
            requestUrl = requestUrl + "&securityCode=" + securityCode;
        }
        if (null != _nextPageToken){
            requestUrl = requestUrl + "&nextPageToken=" + _nextPageToken;
        }

        try {

            String rawResponse = this.getGETResponse(requestUrl,adminAccount);
            if (null != rawResponse) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(rawResponse);
                if (null != root && root.isObject()) {
                    JsonNode commentsNode = root.get("comments");
                    if (null != commentsNode && commentsNode.isArray()) {
                        for (int i = 0; i < ((ArrayNode) commentsNode).size(); i++) {
                            JsonNode commentNode = ((ArrayNode) commentsNode).get(i);
                            _doc.addComment(new AODComment(commentNode));
                        }
                        if (((ArrayNode) commentsNode).size() == 25) {
                            return root.get("nextPageToken").getTextValue(); // If there's more, then return a token for next page
                        } else {
                            return null; // we got less than 25 results so we have seen everything
                        }
                    } else {
                        return null; // There are no results, so no NextPageToken. Return null to end iteration loop
                    }
                }
            }

        } catch (IOException e) {
            logger.warn("IOException in API Request");
            DEFLogManager.LogStackTrace(logger, "AODdocs",e);
        } catch (BadConfigurationException bce){
            logger.warn("BadConfigurationException in API Request : unable to switch to delegated account " + adminAccount);
            DEFLogManager.LogStackTrace(logger, "AODdocs",bce);
        }
        return null;
    }


}
