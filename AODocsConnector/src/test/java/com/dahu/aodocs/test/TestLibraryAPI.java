package com.dahu.aodocs.test;

import com.dahu.aodocs.APIservices.libraryAPI;
import com.dahu.aodocs.types.AODLibrary;
import com.dahu.aodocs.types.AODdocumenttype;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.plugins.core.auth.Google.GoogleCredentialsFactory;
import com.google.auth.oauth2.GoogleCredentials;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
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

public class TestLibraryAPI {

//    private static final String JSON_FILE_PATH = "./src/main/resources/caboffice_defsa.json";
//    private static final String AdminAccount = "vince@dahu.co.uk";
//    private static final String domain = "dahu.co.uk";

    private static final String JSON_FILE_PATH = "./src/main/resources/co-opc-service.json";
    private static final String AdminAccount = "information.manager404@cabinetoffice.gov.uk";
    private static final String domain = "cabinetoffice.gov.uk";

    private GoogleCredentials credential = null;

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
    public void listLibraries(){

        try {
            libraryAPI libraryAPI = new libraryAPI(credential,domain);
            Set<AODLibrary> libraries = libraryAPI.getLibraries(AdminAccount);
            for (AODLibrary library : libraries){
                System.out.println("****************** AODocs Library ******************");
                System.out.println("LibraryID => " + library.getLibraryID());
                System.out.println("Library Name => " + library.getLibraryName());
                System.out.println("Domain => " + library.getDomainName());
                System.out.println("Storage Admin => " + library.getStorageAdmin());
                for (AODdocumenttype type : library.getDocTypes()){
                    System.out.println("\t********** AODocs Document Type ******************");
                    System.out.println("\t -- doc type Name => " + type.getDocTypeName());
                    System.out.println("\t -- doc type ID => " + type.getDocTypeID());
                    System.out.println("\t -- doc type Kind => " + type.getDocTypeID());
                    System.out.println("\t///////////// AODocs Document Type /////////////");
                }
                System.out.println("///////////// AODocs Library /////////////");
            }
        } catch (IOException ioe){
            ioe.printStackTrace();
        } catch (BadConfigurationException bce){
            bce.printStackTrace();
        }


    }

}
