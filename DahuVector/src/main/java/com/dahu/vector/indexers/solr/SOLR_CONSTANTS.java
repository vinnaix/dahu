package com.dahu.vector.indexers.solr;

import com.dahu.core.document.DOCUMENT_CONSTANTS;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 15/03/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class SOLR_CONSTANTS {

    // Field names in Dahu standard Solr config
    static final String SOLRFIELD_ID = DOCUMENT_CONSTANTS.FIELDNAME_ID;
    static final String SOLRFIELD_GUID = DOCUMENT_CONSTANTS.FIELDNAME_GUID;
    static final String SOLRFIELD_URL = DOCUMENT_CONSTANTS.FIELDNAME_URL;
    static final String SOLRFIELD_PARENT_ID = DOCUMENT_CONSTANTS.FIELDNAME_PARENT_ID;
    static final String SOLRFIELD_TITLE = DOCUMENT_CONSTANTS.FIELDNAME_TITLE;
    static final String SOLRFIELD_MIME = DOCUMENT_CONSTANTS.FIELDNAME_MIME_TYPE;
    static final String SOLRFIELD_SIZE = "size";
    static final String SOLRFIELD_CHILD_IDS = DOCUMENT_CONSTANTS.FIELDNAME_CHILD_IDS;
    static final String SOLRFIELD_LAST_MODIFIED_DATE_STR = "last_modified_date_s";
    static final String SOLRFIELD_LAST_MODIFIED_DATE = "last_modified";
    static final String SOLRFIELD_CREATED_DATE_STR = "created_date_s";
    static final String SOLRFIELD_CREATED_DATE = "created_date";
    static final String SOLRFIELD_ALTTITLE = DOCUMENT_CONSTANTS.FIELDNAME_ALT_TITLE;

    static final String SOLRFIELD_AUTHOR = "author";
    static final String SOLRFIELD_OWNER = "owner";
    static final String SOLRFIELD_EXT = DOCUMENT_CONSTANTS.FIELDNAME_EXTENSION;

    static final String SOLRFIELD_LANGUAGE = "language";
    static final String SOLRFIELD_FILENAME = "filename";
    static final String SOLRFIELD_FILEPATH = "filepath";
    static final String SOLRFIELD_SOURCE = "source";
    static final String SOLRFIELD_SERVERNAME = "servername";
    static final String SOLRFIELD_SHARENAME = "sharename";
    static final String SOLRFIELD_METAS = "metas";
    static final String SOLRFIELD_ACL = "acl";
    static final String SOLRFIELD_ALLOW_TOKEN_DOCUMENT = "allow_token_document";
    static final String SOLRFIELD_DENY_TOKEN_DOCUMENT = "deny_token_document";
    static final String SOLRFIELD_ALLOW_TOKEN_PARENT = "allow_token_parent";
    static final String SOLRFIELD_DENY_TOKEN_PARENT = "deny_token_parent";
    static final String SOLRFIELD_ALLOW_TOKEN_SHARE = "allow_token_share";
    static final String SOLRFIELD_DENY_TOKEN_SHARE = "deny_token_share";
    static final String SOLRFIELD_ENTITY = "entity";
    static final String SOLRFIELD_TO = "to";
    static final String SOLRFIELD_FROM = "from";
    static final String SOLRFIELD_CC = "cc";
    static final String SOLRFIELD_SUBJECT = "subject";
    static final String SOLRFIELD_DESCRIPTION = "description";
    static final String SOLRFIELD_KEYWORDS = "keywords";
    static final String SOLRFIELD_CHECKSUM = "checksum";
    static final String SOLRFIELD_SIMHASH = "simhash";

    public static final String SOLR_HIERARCHICALFACET_SEPARATOR = "/";
    public static final String SOLR_HIERARCHICALFACET_PREFIX = "_hier_"; // prefixed to any field going to Solr that should be facetted with a hierarchy

}
