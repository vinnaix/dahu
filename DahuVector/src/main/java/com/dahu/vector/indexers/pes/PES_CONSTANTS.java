package com.dahu.vector.indexers.pes;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 05/09/2018
 * copyright Dahu Ltd 2018
 * <p>
 * Changed by :
 */

public class PES_CONSTANTS {

    public static final String INDEX_PENDING_LOG = "isys.pending.log";
    public static final String GLOBAL_PENDING_LOG = "isys.pending.log";
    public static final String TIMING_LOG = "ISYSIndex.Timings.LOG";
    public static final String WEBAPI_LOG = "isys.webapi.log";
    public static final String ISYS_CFG_FILE = "ISYS.CFG";
    public static final String WEBINDEXES = "webindexes";

    public static final String PES_SEARCH_API_PORT_STR = "8083";
    public static final String PES_WEB_API_PORT_STR = "8700";
    public static final int PES_SEARCH_API_PORT = 8083;
    public static final int PES_WEB_API_PORT = 8700;


    public static final String PES_SEARCH_URL_PREFIX = "/search";
    public static final String PES_QUERY_INDEX_PARAMETER = "IW_DATABASE";
    public static final String PES_WEB_QUERY_FIELD = "IW_FIELD_TEXT";
    public static final String PES_WEB_QUERY_DATE_BEFORE = "IW_FILTER_DATE_BEFORE";
    public static final String PES_WEB_QUERY_DATE_AFTER = "IW_FILTER_DATE_AFTER";
    public static final String PES_BATCH_SIZE = "IW_BATCH_SIZE";
    public static final String PES_FNAME_PARAMETER = "IW_FILTER_FNAME_LIKE";

    public static final String PES_PARTVIEW = "partview";

    public static final String CONFIG_PES_SERVERNAME = "pes_hostname";
    public static final String CONFIG_PES_API_PASSWORD = "pes_api_password";
    public static final String CONFIG_PES_INDEXNAME_TO_INDEX = "pes_index_to_index";
    public static final String CONFIG_PES_INDEXNAME_TO_QUERY = "pes_index_to_query";
    public static final String CONFIG_PES_USERNAME = "username";
    public static final String CONFIG_PES_USER_PASSWORD = "password";
    public static final String CONFIG_PES_ISYS_WEBAPI_PORT = "pes_web_api_port";
}
