package com.dahu.aodocs.APIservices;

import com.dahu.aodocs.AODOCS_CONSTANTS;
import com.dahu.aodocs.types.AODLibrary;
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
 * on 18/11/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class libraryAPI extends DahuAODocsAPIWrapper {

    //libraries(libraryId,name,domainName,storageAdmin,documentTypes,folderDefinition,permissions)
    String domain = null;

    private static final String FIELDS = "libraries(libraryId,name,domainName,storageAdmin,documentTypes)";


    public libraryAPI(GoogleCredentials _creds, String _domain) throws BadConfigurationException, IOException {
        super(_creds);
        this.domain = _domain;
    }


    public libraryAPI(String _securityAPICode, String _domain) throws BadConfigurationException, IOException {
        super(_securityAPICode);
        this.domain = _domain;
    }


    public Set<AODLibrary> getLibraries(String _adminUser){

        Set<AODLibrary> libraries = new HashSet<>();

        try {

            String requestUrl = AODOCS_CONSTANTS.AODOCS_LIBRARY_API_URI + "?domain=" + domain + "&fields=" + FIELDS + "&include=CLASSES";
            if (null != securityCode) {
                requestUrl = requestUrl + "?securityCode=" + securityCode;
            }

            if (null != logger){
                logger.trace("RequestURL = " + requestUrl);
            } else {
                System.out.println("Sending request URL : " + requestUrl);
            }

            String rawResponse = this.getPUTResponse(requestUrl, _adminUser);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(rawResponse);
            if (root.isObject()) {

                JsonNode librariesNode = root.get("libraries");
                if (librariesNode.isArray()) {
                    for (int i = 0; i < ((ArrayNode) librariesNode).size(); i++) {
                        JsonNode libraryNode = ((ArrayNode) librariesNode).get(i);
                        if (null != libraryNode.get("libraryId") ) {
                            AODLibrary library = new AODLibrary(libraryNode);
                            libraries.add(library);
                        }
                    }
                }
            }
        } catch (IOException ioe){
            ioe.printStackTrace();

        } catch (BadConfigurationException bce){
            bce.printStackTrace();
        }

        return libraries;
        }

}
