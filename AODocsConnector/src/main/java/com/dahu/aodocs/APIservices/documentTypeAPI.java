package com.dahu.aodocs.APIservices;

import com.dahu.aodocs.AODOCS_CONSTANTS;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.exception.BadConfigurationException;
import com.google.auth.oauth2.GoogleCredentials;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 02/08/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 * Wrapper for AODocs DocumentType API
 */

public class documentTypeAPI extends DahuAODocsAPIWrapper {


    private String library;
    private String adminAccount;

    /**
     * Construct a documentType API object for AODocs documentType calls using GoogleCredential / OAuth credentials
     * @param _creds GoogleCredential object for a service account
     * @oaram _aodAdmin AODocs Admin user account name
     * @param _library AODocs library Id
     */
    public documentTypeAPI(GoogleCredentials _creds, String _aodAdmin, String _library) throws BadConfigurationException, IOException {
        super(_creds);
        this.adminAccount = _aodAdmin;
        this.library = _library;
    }


    /**
     * Construct a documentType API object for AODocs documentType calls using API code
     * @param _securityAPICode AODocs API code
     * @param _library
     */
    public documentTypeAPI(String _securityAPICode, String _library){
        super(_securityAPICode);
        this.library = _library;
    }

    public Set<String> getDocumentTypes(){

        Set<String>docTypes = new HashSet<>();

        try {

            String requestUrl = AODOCS_CONSTANTS.AODOCS_DOCTYPE_API_URI + "/" + library;
            if (null != securityCode){
                requestUrl = requestUrl + "?securityCode=" + securityCode;
            }

            String rawResponse = this.getGETResponse(requestUrl,adminAccount);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(rawResponse);
            if (root.isObject()){
                JsonNode docTypesNode = root.get("items");
                if  (docTypesNode.isArray()){
                    for (int i = 0; i < ((ArrayNode)docTypesNode).size(); i++){
                        JsonNode docTypeNode = ((ArrayNode)docTypesNode).get(i);
                        if (null != docTypeNode.get("id") && null != docTypeNode.get("displayName")){
                            docTypes.add(docTypeNode.get("id").getTextValue() + ":::" + docTypeNode.get("displayName").getTextValue());
                        }
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
        return docTypes;


    }

}
