package com.dahu.aodocs;


import java.util.Arrays;
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
 */

public class AODOCS_CONSTANTS {

    // CONFIG parameter names
    public static final String CONFIG_AODOCS_SECURITYCODE = "securitycode";
    public static final String CONFIG_LIBRARY_ID = "libraryid";
    public static final String CONFIG_GOOGLESERVICE_JSONFILE_PATH = "google_service_jsonfile_path";
    public static final String CONFIG_AODOCS_STORAGE = "aodocs_storage_account";

    // Assume that all instances of AODocs will use this email alias for their internal account with appropriate accesses to APIs
//    public static final String AODOCS_STORAGE_ACCOUNT = "aodocs-storage@dahu.co.uk";

    // fixed set of fields that exist in all AODocs document classes
    public static final Set<String> defaultFieldNames = new HashSet<>(
            Arrays.asList("classId","className",
                    "richText","initialAuthor","updateAuthor",
                    "versionName","versionDescription","versionAuthor","versionCreated","iconLink")
    );

    // Fixed set of field names / role names in AODocs libraries
    public static final String ATTACHMENTNAME = "attachmentName";
    public static final String PERMISSION = "ao_permission";
    public static final String FOLDERPATH = "folderPath";
    public static final String PERMISSION_READERS = "Readers";

    public static final String AODOCS_SEARCH_API_URI = "https://ao-docs.appspot.com/_ah/api/search/v1/libraries";
    public static final String AODOCS_DOCTYPE_API_URI = "https://ao-docs.appspot.com/_ah/api/documentType/v1/libraries";
    public static final String AODOCS_LIBRARY_API_URI = "https://ao-docs.appspot.com/_ah/api/library/v1";
    public static final String AODOCS_DOCID_API_URI = "https://ao-docs.appspot.com/_ah/api/document/v1beta1";
    public static final String AODOCS_DOCID_LIMIT = "&limit=25"; // request 25 comments at a time

    public static final String GOOGLE_APP_NAME = "Dahu Search";

}
