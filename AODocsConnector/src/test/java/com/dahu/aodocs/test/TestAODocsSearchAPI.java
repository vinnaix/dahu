package com.dahu.aodocs.test;

import com.dahu.aodocs.APIservices.documentTypeAPI;
import com.dahu.aodocs.APIservices.searchAPI;
import com.dahu.aodocs.types.DahuAODocsDocument;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.plugins.core.auth.Google.GoogleCredentialsFactory;
import com.google.auth.oauth2.GoogleCredentials;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 01/10/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class TestAODocsSearchAPI {

    private static final String JSON_FILE_PATH = "./src/main/resources/caboffice_defsa.json";
    private static final String AODocsAdminUser =  "aodocs-storage@dahu.co.uk";

//    private static final String JSON_FILE_PATH = "./src/main/resources/co-opc-service.json";
//    private static final String AODocsAdminUser =  "information.manager404@cabinetoffice.gov.uk";

    private GoogleCredentials credential = null;

    private static final String AODocsLibraryName = "Consulting";
    private static final String AODocsLibraryId = "RR6s3VlySUxJL9rg2k";


    private Set<String> documentTypesInLibrary;

    public TestAODocsSearchAPI() {
        documentTypesInLibrary = new HashSet<>();
    }


    @BeforeSuite
    public void createGoogleCredential(){

        try {
            credential = GoogleCredentialsFactory.createCredential(JSON_FILE_PATH);
            Assert.assertNotNull(credential);
            Assert.assertNotNull(credential.getAccessToken());
            Assert.assertNotNull(credential.getAccessToken().getTokenValue());

        } catch (Exception bce) {
            System.out.println("Error creating GoogleCredentials");
            bce.printStackTrace();
            Assert.assertFalse(true);
        }
    }


    @Test(priority = 1)
    public void getDocumentTypes(){



        try {
            documentTypeAPI documentTypeApi = new documentTypeAPI(credential,AODocsAdminUser, AODocsLibraryId);
            // Get a list of document class IDs from documentType API
            for (String types : documentTypeApi.getDocumentTypes()) {
                // Grab the ID and the NAME for each doc Type we find in this library. ID needed for API, name just for display in Admin
                documentTypesInLibrary.add(types);
            }
            Assert.assertTrue(documentTypesInLibrary.size() > 0);
        } catch (BadConfigurationException bce){
            bce.printStackTrace();
            Assert.fail();
        } catch (IOException ioe){
            ioe.printStackTrace();
            Assert.fail();
        }
    }

    @Test(priority = 2)
    public void testSearch(){


        String docTypeStr = (String)documentTypesInLibrary.toArray()[0];
        Assert.assertTrue(docTypeStr.indexOf(":::")>0);

        String docTypeId = docTypeStr.substring(0,docTypeStr.indexOf(":::"));
        String docTypeName = docTypeStr.substring(docTypeStr.indexOf(":::")+3);

        System.out.println("searching AODocs library, " + AODocsLibraryName + ", for doc type, " + docTypeName);
        try {
            searchAPI search = new searchAPI(credential, AODocsAdminUser,AODocsLibraryId, docTypeId);
            Assert.assertNotNull(search);

            List<DahuAODocsDocument> results = new ArrayList<>();
            int count = 0;

            String pageToken = search.getFirstPageResults(results);

            Assert.assertTrue(results.size() > 0 && results.size() <= 25);
            count = results.size();

            System.out.println("search returned " + results.size());
            System.out.println("requesting another page of results....");
            while (null != pageToken && count < 51){
                results.clear();
                pageToken = search.getResultsPage(pageToken,results);
                count = count + results.size();
                Assert.assertTrue(results.size() > 0 && results.size() <= 25);
                System.out.println("retrieved " + results.size() + " - total retrieved = " + count + " pageToken = " + pageToken);
            }

        } catch (BadConfigurationException bce){
            bce.printStackTrace();
            Assert.fail();
        } catch (IOException ioe){
            ioe.printStackTrace();
            Assert.fail();
        }

    }


}
