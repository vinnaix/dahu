package com.dahu.plugins.edge.walkers;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 15/03/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class WALKER_CONSTANTS {
    protected static final String STARTPOINTS = "startPoints";
    protected static final String INPUTTYPEPRIORITYBUCKET = "PRIORITYREFRESHFOLDER";
    protected static final String INPUTTYPESEEDED = "SEEDED";
    protected static final String INPUTTYPEADDED = "DIRECTORY";
    protected static final String INPUTTYPEOUTPUT = "OUTPUT";
    protected static final String INPUTTYPEDENIED = "DENIED";
    protected static final String INPUTTYPEFILEEXCLUDED = "FILETYPEEXCLUDED";
    protected static final String INPUTFILEEXCLUDED = "FILEEXCLUDED";
    protected static final String INPUTTYPEDIREXCLUDED = "DIREXCLUDED";

    // FS Crawler related
    protected static final String CONFIG_CRAWLERINCLUDETYPES = "includeTypes";
    protected static final String CONFIG_CRAWLEREXCLUDETYPES = "excludeTypes";
    protected static final String CONFIG_CRAWLEREXCLUDEFILES = "excludeFiles";
    protected static final String CONFIG_CRAWLERSTUBFILES = "stubTypes";
    protected static final String CONFIG_CRAWLERROOTS = "startFolders";
    protected static final String CONFIG_REFRESH_MODE = "refresh";
    protected static final String CONFIG_MAX_FILESIZE = "maxfilesize";

    // CIFS related
    protected static final String CONFIG_CIFS_DOMAIN = "cifs_domain";
    protected static final String CONFIG_CIFS_USERNAME = "cifs_username";
    protected static final String CONFIG_CIFS_PASSWORD = "cifs_password";
    protected static final String CONFIG_CIFS_SERVERNAME = "cifs_servername";



    public static final String STATUS_UNCHANGED = "unchanged"; // File has not changed since last crawl - update its lastCrawled time
    public static final String STATUS_REJECT = "reject"; // file exists but crawl rules say we do not want to include it
    public static final String STATUS_FAIL = "fail"; // something went wrong - we cannot find enough information about this file to know what to do with it


    protected static final String CONFIG_CRAWL_DELAY = "crawl_delay_millis"; // delay between documents to slow down overall crawl speed, helps Vector keep up. Default = 1s

    protected static final String CONFIG_REFRESH_DELAY = "refresh_delay_seconds"; // frequency in hours between refreshes starting

    public static final String LEVEL = "level"; // field name used when pushing folders onto the crawl queue to show depth beneath the root level

    public static final String CONFIG_RECOVERYMODE = "recovery";  // If value = "true" do not put any roots on the queue, cos Recovery is underway so crawled folders are on the queue

    public static final String RECOVERY_TEMPFILE_PREFIX = "rebuild_"; // Solr stream output is written to a temp file

    public static final String CONFIG_RECOVERY_SOLR_HOSTNAMEPORT = "solr_host:post"; // Solr service to use for rebuilding
    public static final String CONFIG_RECOVERY_INDEXNAME = "index"; // Solr index to rebuild from
    public static final String CONFIG_RECOVERY_TRIENAME = "trie"; // Trie to rebuild
    public static final String CONFIG_RECOVERY_INCLUDETRIE = "rebuildtrie"; // should Trie rebuild be included?




}
