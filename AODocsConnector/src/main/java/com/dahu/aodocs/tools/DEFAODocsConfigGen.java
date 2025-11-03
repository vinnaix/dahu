package com.dahu.aodocs.tools;

import com.dahu.aodocs.APIservices.libraryAPI;
import com.dahu.aodocs.types.AODLibrary;
import com.dahu.aodocs.types.AODdocumenttype;
import com.dahu.plugins.core.auth.Google.GoogleCredentialsFactory;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
 *
 * Tool to generate a ready-to-use AODocs Connector config file
 *
 * Usage : java -cp ./run/lib/DahuDEFServer.jar:./target/classes PATH_TO_SERVICEACCOUNT_JSON DOMAIN DOMAIN_ADMIN com.dahu.aodocs.tools.DEFAODocsConfigGen
 *
 * where PATH_TO_SERVICEACCOUNT_JSON = path to a JSON file holding private key for a service account
 * DOMAIN = name of Google domain for the AODocs libraries
 * DOMAIN_ADMIN = ID of a user account with privileged access to AODocs libraries to read all data eg a storage account
 *
 * Output - Tool creates a new file with a name based on the domain supplied
 *
 * AODocsLibraries_DOMAIN.json
 *
 * eg for domain = dahu.co.uk, output file = AODocsLibraries_dahu_co_uk.json
 *
 */

public class DEFAODocsConfigGen {

    private static String domain;
    private static String pathJsonFile;
    private static String domainAdmin;

    public static void main(String[] args){

        if (args.length != 3){
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
        BufferedWriter dahuConfigWriter = null;
        try {
            dahuConfigWriter = new BufferedWriter(new FileWriter("AODocsLibraries.json"));

            dahuConfigWriter.write("{\n" +
                "  \"DEFSettings\":{\n" +
                "    \"inputQueues\":[\"libraries\"],\n" +
                "    \"outputQueues\":[\"aodocs_vector\"],\n" +
                "    \"stores\":[\"AODocsConnectorStore\"],\n" +
                "    \"serviceGroup\":\"AODocs\"\n" +
                "    \"runAtStartup\":\"true\"\n" +
            "  },\n" +
                "  \"pluginSettings\":{\n" +
                "    \"interval\":\"30000\",\n" +
                "    \"autoStart\":\"true\",\n" +
                "   \"google_service_jsonfile_path\":  {\"type\": \"env\",\"values\": {\"env_name\": \"GOOGLE_SERVICE_ACCOUNT_JSON_PATH\",\"default\": \"./service.json\"}},\n" +
                "    \"libraries\": [\n"

                    );

            libraryAPI libraryAPI = new libraryAPI(credentials, domain);
            Set<AODLibrary> libraries = libraryAPI.getLibraries(domainAdmin);
            int libraryCounter = 1;
            for (AODLibrary library : libraries){
                dahuConfigWriter.write("    {\n");
                dahuConfigWriter.write("      \"libraryId\":\"" + library.getLibraryID() + "\",\n");
                dahuConfigWriter.write("      \"name\":\"" + library.getLibraryName() + "\",\n");
                dahuConfigWriter.write("      \"domainName\":\"" + library.getDomainName() + "\",\n");
                dahuConfigWriter.write("      \"storageAdmin\":\"" + library.getStorageAdmin() + "\"");
                if (null != library.getDocTypes() && library.getDocTypes().size() > 0) {
                    dahuConfigWriter.write(",\n");
                    int docTypecounter = 1;
                    dahuConfigWriter.write("      \"documentTypes\": {\n");
                    dahuConfigWriter.write("       \"items\": [");
                    for (AODdocumenttype type : library.getDocTypes()) {
                        dahuConfigWriter.write("        {\n");
                        dahuConfigWriter.write("          \"name\":\"" +  type.getDocTypeName()+"\",\n");
                        dahuConfigWriter.write("          \"value\":\"" +  type.getDocTypeID()+"\"\n");
                        dahuConfigWriter.write("        }");
                        if (docTypecounter < library.getDocTypes().size()){
                            dahuConfigWriter.write(",\n");
                        } else {
                            dahuConfigWriter.write("\n");
                        }
                        docTypecounter++;
                    }
                    dahuConfigWriter.write("       ]\n");
                    dahuConfigWriter.write("      }\n");
                } else {
                    dahuConfigWriter.write("\n");
                }
                dahuConfigWriter.write("    }");
                if (libraryCounter < libraries.size()){
                    dahuConfigWriter.write(",\n");
                }
                libraryCounter++;
            }

            dahuConfigWriter.write("  ]\n");
            dahuConfigWriter.write(" }\n");
            dahuConfigWriter.write("}\n");
            dahuConfigWriter.close();
        } catch (IOException ioe){
            ioe.printStackTrace();
            errorExit("Unable to open output file, ./dahuConfigPartial.json");

        } catch (Exception e){
            e.printStackTrace();
            errorExit("Unable to read libraries from domain");
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
        System.err.println("DEFAODocsConfigGen :: Enter pathToJsonFile domain admin");
        System.err.println("DEFAODocsConfigGen :: \t pathToJsonFile = path to a service account Json file");
        System.err.println("DEFAODocsConfigGen :: \t domain = AODocs/Google Domain");
        System.err.println("DEFAODocsConfigGen :: \t admin = account name for an account with Admin/Super-user privileges in AODocs domain");
        System.exit(-1);

    }

}
