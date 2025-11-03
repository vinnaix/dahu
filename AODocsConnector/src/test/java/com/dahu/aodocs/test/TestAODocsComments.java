package com.dahu.aodocs.test;

import com.dahu.aodocs.APIservices.documentIdAPI;
import com.dahu.aodocs.types.DahuAODocsDocument;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.plugins.core.auth.Google.GoogleCredentialsFactory;
import com.google.auth.oauth2.GoogleCredentials;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 07/10/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class TestAODocsComments {

    private static final String JSON_FILE_PATH = "./src/main/resources/caboffice_defsa.json";
    private GoogleCredentials credential = null;

    private String documentId = "RR6yW2Kz51twvu1FNo";
    private static final String AODocsAdminUser =  "aodocs-storage@dahu.co.uk";

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

    @Test
    public void getComments(){

        Assert.assertNotNull(credential);

        try {

            ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("id", documentId);
            node.put("domainName", "dahu.co.uk");
            node.put("libraryName", "Test Library");

            // Create new DahuAODocs Doc with the fixed test doc id
            DahuAODocsDocument doc = new DahuAODocsDocument(node);

            // We need to call the AODocs DocumentID API to look up our document from its ID
            // Instantiate our wrapper class with a valid GoogleCredentials object
            documentIdAPI docIdApi = new documentIdAPI(credential,AODocsAdminUser);
            // Now run a search against AODocs DocumentID API to find the document in AODocs-land
            // Read all the comments that exist in AODocs-land
            // Write all the comments to our DahuAODocs Doc wrapper
            docIdApi.readAndSetComments(doc);

            // Our test document has four comments. Assuming these are never deleted, we should always find at least 4 comments.
            // Since anyone can add more comments, we cannot know how many to expect, but we can look for a minimum number.
            Assert.assertTrue(doc.getComments().size() > 3);
            System.out.println("AODocs document was found to have " + doc.getComments().size() + " comments attached to it");
        } catch (BadConfigurationException bce){
            bce.printStackTrace();
            Assert.fail();
        } catch (IOException ioe){
            ioe.printStackTrace();
            Assert.fail();
        }

    }




}
