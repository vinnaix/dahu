package com.dahu.aodocs.tools;

import com.dahu.plugins.core.auth.Google.GoogleCredentialsFactory;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.File;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 16/12/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class GoogleCredsTest {


    private static String domain;
    private static String pathJsonFile;
    private static String domainAdmin;


    public static void main(String[] args) {

        if (args.length != 3) {
            errorExit("Wrong number of arguments");
        } else {
            parseArgs(args);
        }

        System.out.println("Reading all AODocs libraries in " + domain + " :: using " + domainAdmin + " account to list libraries");
        GoogleCredentials credentials = null;
        try {
            credentials = GoogleCredentialsFactory.createCredential(pathJsonFile);
        } catch (Exception bce) {
            System.out.println("Error creating GoogleCredentials");
            bce.printStackTrace();
            errorExit("Unable to create GoogleCredentials");
        }


        try {
            GoogleCredentials delegatedCreds = GoogleCredentialsFactory.getDelegatedCredentials(credentials, domainAdmin);



            System.out.println("successfully delegated to " + domainAdmin);
        } catch (Exception e){
            e.printStackTrace();
        }
    }


    private static void parseArgs(String[] _args){

        pathJsonFile = _args[0];
        domain = _args[1];
        domainAdmin = _args[2];

        if (null == pathJsonFile || null == domainAdmin || null == domain ){
            errorExit("One of arguments missing");
        }
        File f = new File(pathJsonFile);
        if (! (f.exists() && f.canRead() )){
            errorExit("Unable to file JSON file at " + pathJsonFile);
        }

    }

    private static void errorExit(String _message){

        System.err.println(_message);
        System.err.println("GoogleCredsTest :: Enter pathToJsonFile domain account");
        System.err.println("GoogleCredsTest :: \t pathToJsonFile = path to a service account Json file");
        System.err.println("GoogleCredsTest :: \t domain = AODocs/Google Domain");
        System.err.println("GoogleCredsTest :: \t account = account name for an account with Admin/Super-user privileges in AODocs domain");
        System.exit(-1);

    }


}
