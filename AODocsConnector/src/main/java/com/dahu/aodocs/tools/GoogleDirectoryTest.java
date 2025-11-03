package com.dahu.aodocs.tools;

import com.dahu.plugins.core.auth.Google.GoogleCredentialsFactory;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.model.Group;
import com.google.api.services.admin.directory.model.Groups;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.File;
import java.util.Arrays;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 16/12/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class GoogleDirectoryTest {

    private static final String GOOGLE_SCOPE_READGROUP = "https://www.googleapis.com/auth/admin.directory.group.readonly";
    private static final String GOOGLE_SCOPE_READUSER = "https://www.googleapis.com/auth/admin.directory.user.readonly";

    private static String domain;
    private static String pathJsonFile;
    private static String domainAdmin;
    private static String userToLookup;

    public static void main(String[] args) {

        if (args.length != 4) {
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

            delegatedCreds = delegatedCreds.createScoped(Arrays.asList(GOOGLE_SCOPE_READGROUP,GOOGLE_SCOPE_READUSER));
            System.out.println("GoogleAuthenticator:- getGoogleGroups - delegating to " + domainAdmin + " and requesting scopes, " + GOOGLE_SCOPE_READGROUP + " and " + GOOGLE_SCOPE_READUSER);

            final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter((com.google.auth.Credentials)delegatedCreds);

            Directory service = null;
            Groups groups = null;
            try {
                service = new Directory.Builder(HTTP_TRANSPORT, JacksonFactory.getDefaultInstance(), requestInitializer)
                        .setApplicationName("Dahu Search App")
                        .build();
            } catch (Exception e){
                e.printStackTrace();
            }

            if (null != service && null != service.groups()) {
                groups = service.groups().list()
                        .setUserKey(userToLookup)
                        .setMaxResults(2)
                        .execute();
            }
            if (null != groups && null != groups.getGroups()) {
                System.out.println("looked up some groups for " + userToLookup);
                for (Group g : groups.getGroups()) {
                    System.out.println("group id = " + g.getEmail());
                }
            } else {
                System.out.println("Groups is null for " + userToLookup);
            }

        } catch (Exception e){
            e.printStackTrace();
        }
    }


    private static void parseArgs(String[] _args){

        pathJsonFile = _args[0];
        domain = _args[1];
        domainAdmin = _args[2];
        userToLookup = _args[3];

        if (null == pathJsonFile || null == domainAdmin || null == domain || null == userToLookup){
            errorExit("One of arguments missing");
        }
        File f = new File(pathJsonFile);
        if (! (f.exists() && f.canRead() )){
            errorExit("Unable to file JSON file at " + pathJsonFile);
        }

    }

    private static void errorExit(String _message){

        System.err.println(_message);
        System.err.println("GoogleDirectoryTest :: Enter pathToJsonFile domain account");
        System.err.println("GoogleDirectoryTest :: \t pathToJsonFile = path to a service account Json file");
        System.err.println("GoogleDirectoryTest :: \t domain = AODocs/Google Domain");
        System.err.println("GoogleDirectoryTest :: \t account = account name for an account with Admin/Super-user privileges in AODocs domain");
        System.err.println("GoogleDirectoryTest:: \t user account  = account name to look up groups for");
        System.exit(-1);

    }


}
