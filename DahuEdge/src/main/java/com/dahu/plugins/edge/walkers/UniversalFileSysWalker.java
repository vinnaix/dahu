package com.dahu.plugins.edge.walkers;

import com.dahu.core.document.DEFDocument;
import com.dahu.core.document.DEFFileDocument;
import com.dahu.core.document.DOCUMENT_CONSTANTS;
import com.dahu.core.exception.BadDocumentException;
import com.dahu.core.exception.MissingFileException;
import com.dahu.core.interfaces.iDocument;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.core.trie.TrieBranch;
import com.dahu.core.trie.TrieLeaf;
import com.dahu.core.utils.ConfigUtils;
import com.dahu.def.annotations.DEFAnnotationMT;
import com.dahu.def.config.PluginConfig;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.exception.MQException;
import com.dahu.def.plugins.ListeningService;
import com.dahu.def.types.MQueue;
import com.dahu.def.types.Service;
import com.dahu.def.types.Sitrep;
import com.dahu.plugins.edge.walkers.storage.ProtectedTrie;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.jms.JMSException;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.dahu.core.document.DOCUMENT_CONSTANTS.ACTION_DELETE;
import static com.dahu.plugins.edge.walkers.WALKER_CONSTANTS.*;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 06/11/2017
 * copyright Dahu Ltd 2017
 * <p>
 * Changed by :
 *
 * A Crawler for both FileSys and CIFS file systems.
 *
 * On startup, the root folders defined in config are added to a queue (seedTheQueues() method)
 * TODO find a startup procedure that doesn't add root folders to the queue in every thread in every instance
 *
 *  TextMessages encapsulating a FOLDER are pushed to the crawl Queue
 *  The message format is JSON and must follow the pattern shown in these examples
 *
 *  { "id":"smb://server/share/path","level":"INTEGER" }
 *  { "id":"D:/folder/folder/folder","level":"INTEGER" }
 *  { "id":"/folder/path","level":"INTEGER" }
 *  { "id":"./folder/folder/path","level":"INTEGER" }
 *  { "id":"\\server\share\path\folder","level":"INTEGER" }
 *
 * The crawler reads a message from the crawl queue and resolves to a folder on a file-system or a share
 * The crawler checks if it can process the folder, according to the crawl rules in config
 * If it can, it processes THREE levels of sub-folder beneath the folder provided in the message
 * Any FILES found in any of the three levels of hierarchy are pushed as Documents to the output queue, which should be linked to a Vector
 * The files are NOT read into memory at this stage, so the output queue receives just a TextMessage containing details of the document metadata
 * Any child folders or sub-folders are processed
 * Any sub-folders of the sub-folders (ie three levels below the original folder supplied in the message) are themselves pushed on
 * to the crawl queue to be picked up by another thread and processed.
 *
 */

@DEFAnnotationMT(isMultiThreaded="true")
public class UniversalFileSysWalker extends ListeningService {

    protected boolean isSeeded = false;  // have we pushed some crawl root folders onto the queue so we can start?

    // separate loggers for different functions
    protected Logger insertsLog;
    protected Logger updatesLog;
    protected Logger deletesLog;
    protected Logger unchangedLog = null;

    protected int crawlDelay = 500; // default - delay of 0.5s between processing a crawl folder, to avoid overloading Vector queue

    protected int refreshDelay = 600; // default - delay between starting a refresh thread in seconds

    protected boolean forceRefresh = false; // flag - should crawler push crawl roots on queue, or just pick up some work?

    // Crawl rules
    protected Set<String> includeTypePatterns = new HashSet<>();
    protected Set<String> excludeTypePatterns = new HashSet<>();
    protected Set<String> excludeFilePatterns = new HashSet<>();
    protected int maxFileSize = 256*1024*1024; // do not crawl files larger than this - default = 256MB

    protected String source = null;   // String to insert as metadata to all items processes. Should identify this connector by name

    protected ProtectedTrie crate = null; // for navigation within a trie, and comparison with existing data.

    // SMB data coming from config
    protected String smb_servername = null;
    protected String smb_domain = null;
    protected String smb_username = null;
    protected String smb_password = null;
    protected NtlmPasswordAuthentication auth = null;

    private static final String QUEUEREADYNMSG = "Listening for messages";

    protected MQueue crawlQueue; // a FS walker can only have ONE input Queue so here it is
    protected MQueue.MQueueSession crawlQueueSession; // session on the input queue
    protected MQueue vectorQueue; // output queue
    protected MQueue.MQueueSession vectorQueueSession; // output queue session

    protected boolean isRecoveryMode = false; // if true, do not add roots to crawlqueue

    // thread pool for the scheduled tasks that will run hoursly, every 4 hours, every 24 hours and every 7 days
    // to push various buckets of folders onto the queue to be refreshed

    private Set<String> trieNodeBranchToInsert = new HashSet<>(); // placeholder to accumulate set of folder paths that need to be inserted into the crate at suitable time
    private Set<String> trieNodeLeavesToInsert = new HashSet<>(); // placeholder to accumulate set of file paths that need to be inserted into the crate at suitable time; format filepath_:_lastmod:_:_size
    private Set<String> trieNodesToDelete = new HashSet<>(); // placeholder to accumulate set of file paths that need to be deleted from the crate at suitable time

    // Thread pool to run periodically to push roots on to queue for refresh crawl
    ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(5);

    /**
     * Constructor for a Generic Walker. Set up the input and outout queues (all crawlers will need a crawl queue plus a queue to put output tasks on_
     * @param _level  Log level for the Loggers
     * @param _instance The Instance object that contains config for this InstancePlugin. we need to read its properties
     * @throws JAXBException JAXBException
     * @throws MQException MQException
     * @throws BadConfigurationException BadConfigurationException
     * @throws JMSException JMSException
     */
    public UniversalFileSysWalker(Level _level, Service _instance, int _threadNumber) throws JAXBException, MQException, BadConfigurationException, JMSException {
        super(_level, _instance,_threadNumber);

        logger.trace("New FileSys Walker");
        source = "crawler:"+this.serviceName;
        // separate data loggers to record all the inserts, updates and deletes (and optionally unchanged)
        insertsLog = DEFLogManager.getLogger(_instance.getName()+"-INSERTS",_level);
        updatesLog = DEFLogManager.getLogger(_instance.getName()+"-UPDATES",_level);
        deletesLog = DEFLogManager.getLogger(_instance.getName()+"-DELETES",_level);
        if (_level.equals(Level.TRACE)) {
            unchangedLog = DEFLogManager.getLogger(_instance.getName() + "-UNCHANGED", _level);
        }

        String maxFileSizeConfig = PluginConfig.getPluginProperties(serviceName).getPropertyByName(WALKER_CONSTANTS.CONFIG_MAX_FILESIZE);
        if (null != maxFileSizeConfig){
            try {
                maxFileSize = Integer.parseInt(maxFileSizeConfig);
            } catch (NumberFormatException nfe){
                logger.warn("error in config - maxFileSize is not an integer. Setting to default value of 256MB");
            }
        }

        // Queues have already been set up in the parent constructor for ServicePluginBase
        // However, a FileSys crawler has specific requirements for crawlers - needs ONE input and ONE output
        if (inputQueues.size() != 1){
            throw new BadConfigurationException("FileSys crawler must have exactly ONE input queue for crawling");
        }
        if (outputQueues.size() != 1){
            throw new BadConfigurationException("FileSys crawler must have exactly ONE output queue for vector");
        }

        // Use the existing Queues and Sessions - just rename them here to make it easier to reference them
        crawlQueue = this.getFirstInputQueue();
        if (null == crawlQueue){
            throw new BadConfigurationException("Crawl queue not defined - must be one input queue");
        }
        crawlQueueSession = crawlQueue.getSession(serviceName); // all threads share the same crawl queue
        logger.trace("Setting crawlQueue to use " + crawlQueue.getQueueName());

        vectorQueue = this.getFirstOutputQueue();
        if (null == vectorQueue){
            throw new BadConfigurationException("Vector queue not defined - must be one output queue");
        }
        vectorQueueSession = vectorQueue.getSession(serviceName);
        logger.trace("Setting vectorQueue to use " + vectorQueue.getQueueName());


        if (crawlQueueSession == null || vectorQueueSession == null){
            throw new BadConfigurationException("Unable to start service because no session exists on either crawl queue or vector queue");
        }

        //CONFIG_RECOVERYMODE
        if (PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_RECOVERYMODE) != null) {
            if (PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_RECOVERYMODE).equals("true")){
                isRecoveryMode = true;
            }
        }

        if (PluginConfig.getPluginProperties(serviceName).getPropertiesByName(CONFIG_CRAWLERINCLUDETYPES) != null){
            for (String s : PluginConfig.getPluginProperties(serviceName).getPropertiesByName(CONFIG_CRAWLERINCLUDETYPES)){
                try {
                    Pattern.compile(s);
                    includeTypePatterns.add(s);
                } catch (PatternSyntaxException pse){
                    logger.warn("Invalid regex pattern in crawler INCLUDE TYPE list :- " + s);
                }
            }
        }

        if (PluginConfig.getPluginProperties(serviceName).getPropertiesByName(CONFIG_CRAWLEREXCLUDETYPES) != null){
            for (String s : PluginConfig.getPluginProperties(serviceName).getPropertiesByName(CONFIG_CRAWLEREXCLUDETYPES)){
                try {
                    Pattern.compile(s);
                    excludeTypePatterns.add(s);
                } catch (PatternSyntaxException pse){
                    logger.warn("Invalid regex pattern in crawler EXCLUDE TYPE list :- " + s);
                }
            }
        }

        if (PluginConfig.getPluginProperties(serviceName).getPropertiesByName(CONFIG_CRAWLEREXCLUDEFILES) != null){
            for (String s : PluginConfig.getPluginProperties(serviceName).getPropertiesByName(CONFIG_CRAWLEREXCLUDEFILES)){
                try {
                    Pattern.compile(s);
                    excludeFilePatterns.add(s);
                } catch (PatternSyntaxException pse){
                    logger.warn("Invalid regex pattern in crawler EXCLUDE FILE PATTERN list :- " + s);
                }

            }
        }

//        CONFIG_CRAWL_DELAY
        if (PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_CRAWL_DELAY) != null) {
            String crawlDelayStr = PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_CRAWL_DELAY);
            try {
                crawlDelay = Integer.parseInt(crawlDelayStr);
                logger.debug("Setting crawl delay to " + crawlDelay);
            } catch (NumberFormatException nfe){
                logger.warn("Bad config - unable to set crawl delay to " + crawlDelayStr + " expected an integer, delay factor in milliseconds");
            }
        }

        // refresh delay
        //CONFIG_REFRESH_DELAY
        if (PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_REFRESH_DELAY) != null) {
            String refreshDelayStr = PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_REFRESH_DELAY);
            try {
                refreshDelay = ConfigUtils.timeParser(refreshDelayStr);
                logger.trace("Setting refresh cycle to " + refreshDelay + " seconds");
            } catch (NumberFormatException nfe){
                logger.warn("Invalid config entry : " + CONFIG_REFRESH_DELAY + " : " + refreshDelayStr + " not a valid time internal. Setting to 3600s");
                refreshDelay = 3600;
            }

        }

        if (PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_CIFS_SERVERNAME) != null){
            smb_servername = PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_CIFS_SERVERNAME);
        }
        if (PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_CIFS_DOMAIN) != null){
            smb_domain = PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_CIFS_DOMAIN);
        }
        if (PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_CIFS_USERNAME) != null){
            smb_username = PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_CIFS_USERNAME);
        }
        if (PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_CIFS_PASSWORD) != null){
            smb_password = PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_CIFS_PASSWORD);
        }

        if (PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_REFRESH_MODE) != null){
            if (PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_REFRESH_MODE).equalsIgnoreCase("true")) {
                forceRefresh = true;
            }
        }


        if (smb_domain != null && smb_username != null && smb_password != null){
            auth = new NtlmPasswordAuthentication(smb_domain, smb_username, smb_password);
            if (null == auth){
                throw new BadConfigurationException("unable to create new NtlmPasswordAuthentication ");
            }
            logger.trace("SMB parameters : domain = " + smb_domain + " : username = " + smb_username);
        }

        logger.trace("Now look for the crate for " + serviceName);
        if (null != PluginConfig.getFirstStore(serviceName)){
            logger.trace("The crate for " + serviceName + " is of type, " + PluginConfig.getFirstStore(serviceName).getClass().getCanonicalName());
        } else {
            logger.trace("The crate for " + serviceName + " is null");
        }
        // Load the crate/Trie
        if (PluginConfig.getFirstStore(serviceName) instanceof ProtectedTrie) {
            logger.trace("Opening crate for " + serviceName + " :: Trie Name " + ((ProtectedTrie) PluginConfig.getFirstStore(serviceName)).getReadOnlyCrate().getName());
            crate = (ProtectedTrie) PluginConfig.getFirstStore(serviceName);
            logger.trace("Finished opening crate.");
        } else {
            throw new BadConfigurationException("FileSysWalker instance has been given wrong type of Store. Expected ProtectedTrie - receieved " + PluginConfig.getFirstStore(serviceName).getClass().getCanonicalName());
        }

        // tell the world we are ok
        this.setMessage(QUEUEREADYNMSG);

        logger.trace("Finished setting up crawler");

        if (scheduledExecutorService.isShutdown()){
            scheduledExecutorService = Executors.newScheduledThreadPool(5);
        }

        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                Set<String> roots = new HashSet<>();
                // push the roots on to queue
                if (PluginConfig.getPluginProperties(serviceName).getPropertiesByName(CONFIG_CRAWLERROOTS) != null){
                    for (String s : PluginConfig.getPluginProperties(serviceName).getPropertiesByName(CONFIG_CRAWLERROOTS)){
                        roots.add(s);
                    }
                }
                synchronized (myLock){
                    try {
                        seedTheQueue(roots);
                    } catch (Exception e){
                        logger.warn("Scheduled Task to re-push crawl roots  failed");
                        DEFLogManager.LogStackTrace(logger, "UniversalFSWalker:ScheduledTask",e);
                    }
                }

            }
        }, refreshDelay,refreshDelay, TimeUnit.SECONDS);

        // Now we are ready to do our thang
    }

    /**
     * Gets called ONCE per service.
     */
    @Override
    public void onStartofService() throws JAXBException, MQException, BadConfigurationException, JMSException{
        super.onStartofService();
        Set<String> roots = new HashSet<>();
        // push the roots on to queue
        if (PluginConfig.getPluginProperties(serviceName).getPropertiesByName(CONFIG_CRAWLERROOTS) != null){
            for (String s : PluginConfig.getPluginProperties(serviceName).getPropertiesByName(CONFIG_CRAWLERROOTS)){
                roots.add(s);
            }
        }
        synchronized (myLock){
            if (! isSeeded){
                seedTheQueue(roots);
                isSeeded = true;
            }
        }
    }


    /**
     * Process a message from crawl queue. Message *SHOULD* define a path to a FOLDER, otherwise we cannot crawl
     * This method will process all the files in the folder, and recurse through two levels of sub-folders
     * Any FOLDERS found at three levels below the starting folder will be added to the crawl queue
     * The message also defines a parameter called "depth" which represents the number of sub-folders from root
     * Depth is not currently used but could be used to control how far to recurse a deep file system
     * @param _message a Json String containing an "id" and a "depth" parameter where id defines a path to a  FOLDER to crawl
     */

    public void processMessage(String _message){


        // Slow things down - let's wait each time we see a new folder to crawl
        try {
            Thread.sleep(crawlDelay);
        } catch (InterruptedException ie){

        }

        String jsonMsg = _message;
        String filePath = null;
        int level = -1;
        // new file/folder
        DEFFileDocument f = null;


        ObjectMapper mapper = new ObjectMapper();
        JsonNode msgNode;
        try {
            msgNode = mapper.readTree(jsonMsg);
        } catch (IOException ioe){
            logger.warn("Unable to read JSON from incoming text message. Message is \"" + jsonMsg + "\"");
            return; // nothing more to do with this message
        }


        try {
            filePath = msgNode.get("id").getTextValue();
            level = msgNode.get("level").getIntValue();
        } catch (NullPointerException npe) {
            logger.warn("Json Message on crawler input queue has no filepath attribute");
            return;
        }
        logger.trace("Read entry from queue - path = " + filePath);

        if (null != filePath) {
            setMessage("Processing folder, " + filePath);


            boolean canProcess = filterFile(filePath);

            try {
                if (filePath.startsWith("smb:")) {
                    logger.trace("Creating new SmbFile from " + filePath + " SMB parameters : domain = " + smb_domain + " : username = " + smb_username + " : password = " + smb_password);
                    logger.trace("Auth is " + auth);
                    f = new DEFFileDocument(new SmbFile(filePath, auth), source, smb_domain,smb_username,smb_password);
                } else if (filePath.startsWith("file://")){
                    f = new DEFFileDocument(new File(filePath.substring(7)),source);
                } else {
                    logger.trace("Opening File for " + filePath);
                    f = new DEFFileDocument(new File(filePath),source);
                }
            } catch (MalformedURLException murle){
                // folder from crawl queue no longer exists
                logger.warn("Walker pulled CIFS folder off the queue but error while opening it - " + filePath);
                DEFLogManager.LogStackTrace(logger,"FSWalker:ProcessMessage",murle);
                if (crate.getReadOnlyCrate().getNode(filePath) != null) {
                    trieNodesToDelete.add(filePath);
                }
                return;
            } catch (MissingFileException mfe) {
                // folder from crawl queue no longer exists
                logger.warn("Walker pulled folder off the queue but no folder exists. " + filePath);
                if (crate.getReadOnlyCrate().getNode(filePath) != null) {
                    trieNodesToDelete.add(filePath);
                }
            } catch (BadDocumentException bdfe) {
                // folder from crawl queue no longer exists
                logger.warn("Walker pulled folder off the queue but error while opening it - " + filePath);
                DEFLogManager.LogStackTrace(logger,"FSWalker:ProcessMessage",bdfe);
                if (crate.getReadOnlyCrate().getNode(filePath) != null) {
                    trieNodesToDelete.add(filePath);
                }
                return;
            }
            if (null != f && f.exists()) {
                logger.debug("Crawler read a folder form queue - processing " + f.getAbsolutePath());
            }
        }

        int retryCount = 0;

        if (f != null) {
            if (!f.isDir()) {
                // someone put a file on the queue - bad juju - only folders allowed
                logger.warn("Walker pulled a file off the queue. Big NONO. Only FOLDERS are allowed on the queue" + f.getAbsolutePath());
            } else if (!f.exists()) {
                logger.warn("Walker pulled folder off the queue but no folder exists. " + f.getAbsolutePath());
                if (crate.getReadOnlyCrate().getNode(filePath) != null) {
                    trieNodesToDelete.add(filePath);
                }
            } else if (!f.canRead()) {
                // Might be a problem with auth - should report in logs, but we cannot carry on. SHould not assume we delete this from crates
                logger.error("CIFS Walker not authorised to open this folder " + f.getAbsolutePath());
            } else if (crate.getReadOnlyCrate().getNode(filePath) == null) {
                logger.trace("Crawling new folder - " + filePath);
                // folder is good and we can read it - but we have not seen this folder before in the crates
                // we are not refreshing it - we are crawling a new folder and sub-folders
                trieNodeBranchToInsert.add(filePath);

                // If we have a problem with queues, we should wait then try again
                // the QueueWatcher event might be able to fix it
                boolean retry = true;
                while (retry && retryCount < 5) {
                    try {
                        crawlNewFolder(f, level);
                        retry = false; // we are done - no more looping
                    } catch (JMSException jmse) {
                        // problem posting to a queue
                        logger.warn("Problem with crawl queue : " + jmse.getLocalizedMessage());
//                        DEFLogManager.LogStackTrace(logger,"UFSW crawlNewFolder",jmse);
                        retryCount++;
                    } catch (MQException mqe) {
                        // problem posting to a queue
                        logger.warn("Problem with crawl queue : " + mqe.getLocalizedMessage());
//                        DEFLogManager.LogStackTrace(logger,"UFSW crawlNewFolder",mqe);
                        retryCount++;
                    }
                }


            } else {
                boolean retry = true;
                while (retry && retryCount < 5) {

                    // we have seen this file or folder before - it is in the crates
                    // Let's start crawling again from here
                    logger.trace("This folder exists in Trie. Crawl its child folders");
                    try {
                        // The File object is for a Folder so we can cast it's mirror from the Trie to a TrieBranch - it cannot be a Leaf
                        checkCrateFolderForChildFolders((TrieBranch)crate.getReadOnlyCrate().getNode(filePath), auth, level);
                        retry = false; // we are done - no more looping
                    } catch (JMSException jmse) {
                        // problem posting to a queue
                        logger.warn("Problem with crawl queue : " + jmse.getLocalizedMessage());
                        DEFLogManager.LogStackTrace(logger,"UFSW crawlNewFolder",jmse);
                        retryCount++;
                    } catch (MQException mqe) {
                        // problem posting to a queue
                        logger.warn("Problem with crawl queue : " + mqe.getLocalizedMessage());
                        DEFLogManager.LogStackTrace(logger,"UFSW crawlNewFolder",mqe);
                        retryCount++;
                    }
                }

            }
            if (retryCount >=5){
                logger.warn("Attempted to crawl a folder and got a problem with queues on five occasions. Stopping the service.");
                this.setMessage("Problem with queue");
                this.setSitrep(Sitrep.ERROR);
                this.stop();
            }
        }

        // Finished processing a Folder from the queue - now insert to crate
        crate.insertBranches(trieNodeBranchToInsert);
        crate.insertLeaves(trieNodeLeavesToInsert);
        crate.deleteNodes(trieNodesToDelete);


        trieNodeBranchToInsert.clear();
        trieNodeLeavesToInsert.clear();
        trieNodesToDelete.clear();
    }


    /**
     * a method to read startpoints from config and add them to the input queue
     * OR read from a priority crate if one exists, and add to input queue
     * @throws BadConfigurationException
     */
    protected void seedTheQueue(Set<String> _roots) throws  BadConfigurationException, MQException, JMSException {
        if (isRecoveryMode){
            logger.info("Recovery Mode : Not pushing any roots on crawl queue");
            return;
        }

        if (crawlQueue.getcurrentMessageCount() > 0){
            logger.debug("Crawl in progress - restarting with existing crawl queue");
            return;
        }

        logger.trace("Crawler seeding roots onto queue");

        if (_roots != null && _roots.size() > 0){

            if (null == crawlQueue || null == crawlQueueSession) {
                // Use the existing Queues and Sessions - just rename them here to make it easier to reference them
                crawlQueue = this.getFirstInputQueue();
                if (null == crawlQueue) {
                    throw new BadConfigurationException("Crawl queue not defined - must be one input queue");
                }
                crawlQueueSession = crawlQueue.getSession(serviceName); // all threads share the same crawl queue
            }

            for (String sp : _roots){

                // File crawler roots can be "smb://path" [CIFS filepath] "\\server\path" [UNC] for windows share, "C:\\path" [Windows drive], "C:/path" [Windows drive] or "/path" [Linux/Mac]
                logger.debug("FS Walker processing root folder : " + sp);
                if (sp.startsWith("smb:")){
                    // CIFS filepath
                    // just in case someone gives us an SMB file ref with backslashes....
                    if (sp.startsWith("smb:\\")){
                        sp = sp.replaceAll("\\\\","/");
                    }
                } else if (sp.startsWith("\\\\")){
                    //UNC
                    // Turn it into an SMB ref
                    while (sp.startsWith("\\")){
                        sp = sp.substring(1);
                    }
                    sp = sp.replaceAll("\\\\","/");
                    sp = "smb://" + sp;

                } else if (sp.startsWith("/")){
                    // Linux/Mac - do nothing to rewrite the path - just prefix it so it starts file:///
                    sp = "file://" + sp;
                } else if (Pattern.matches("^[a-zA-Z]:",sp)){
                    // Windows drive
                    // Normalize any slashes as forward-slashes - start by looking from the drive root D:\
                    if (Pattern.matches("^[a-zA-Z]:\\\\",sp)){
                        sp = sp.replaceAll("\\\\","/");
                    }
                    sp = "file://" + sp;
                }


                if (! sp.endsWith("/")){
                    sp = sp + "/";
                }

                ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
                node.put("id" ,sp);
                node.put("level",0);
                logger.trace("Attempting to push root to queue : " + node.toString());
                if (!crawlQueueSession.postTextMessage(node.toString())){
                    logger.warn("Failed to put root folder on queue : instance=" + serviceName+" queue="+crawlQueue.getQueueName());
                } else {
                    logger.debug("Pushed root folder, " + sp + " onto input queue : " + crawlQueue.getQueueName());
                }
            }
        }
    }


    /**
     * Check whether this file path meets the criteria for include and does not meet criteria for exclude
     * patterns are supplied as simple regexes and tested against the full path to a file
     * @param _path - full path to a file
     * @return true if this file can be indexed - false if it should be excluded
     */
    private boolean filterByFileName(String _path){

        boolean canInclude = true;

        if (null != excludeFilePatterns) {
            for (String ssn : excludeFilePatterns) {
                if (_path.matches(ssn)) {
                    canInclude = false;
                    logger.debug("Rejecting file due to file name exclusions - " + _path + " failed against rule " + ssn);
                    break;
                }
            }
        }
        return canInclude;
    }

    /**
     * Check whether this file should be excluded based in its extension
     * @param _path - full path to a file
     * @return true if this file type should be sent to be indexed - false if it should be skipped
     */
    private boolean filterByFileType(String _path){
        // filtering rules:
        // - includes are applied first, and then any exclude rules
        // - instance rules are used in preference to default rules
        // - includeType of NULL is assumed to be all fileTypes
        // -  empty includeType can be used to override a default includeType to include all files.

        if (_path.lastIndexOf(".")!=-1 && _path.lastIndexOf(".")+1 < _path.length()) {
            String extension = _path.substring(_path.lastIndexOf(".")+1);


            // null means we don't have that list,
            // size zero means we have an empty list - which we treat as a match

            // Check if it's one we can include - if there are no include Types set, then all types are good
            if (includeTypePatterns == null || includeTypePatterns.size() == 0 || includeTypePatterns.contains(extension)){
                if (excludeTypePatterns == null || ! excludeTypePatterns.contains(extension)){
                    return true;
                } else {
                    logger.debug("filterFileTypes exclude list doesn't like extension, \"" + extension + "\" : excluding " + _path);
                    return false;
                }
            } else {
                logger.debug("filterFileTypes include list doesn't like extension, \"" + extension + "\" : excluding " + _path);
                return false;
            }
        }
        // Default - if a file path does not have an extension, we include it
        return true;
    }


    /**
     * Filter out files by comparison to config rules for permitted names and types
     *
     * @param _name String - can be a file name or a full path
     * @return true if the file name/path passes checks for file name and file extension
     */
    private boolean filterFile(String _name){
        String path = _name;
        if (path.endsWith("/")){
            path = path.substring(0,path.length()-1);
        }
        return (filterByFileName(path) && filterByFileType(path));
    }



    /**
     * Recursive function to iterate from a TrieNode in a crates
     * Find all the files associated with the node - push them to the DELETES queue so they are removed from index
     * Find all the folders under the current node - send them to this function
     * @param _root a TrieNode representing a folder in a tree FS that no longer exists
     * @throws MQException exception thrown if unable to push entry to DELETES queue
     */
    protected void deleteFolderAndChildren(TrieBranch _root) throws MQException, JMSException{

        deletesLog.info("DELETE called on " + _root.getFullPath() + " - sending to DELETE queue for all files under this folder");
        trieNodesToDelete.add(_root.getFullPath()); // remove this node from the crate

        List<TrieLeaf> childFiles = _root.getFiles();
        iDocument childFile = null;
        try {
            // push  DELETE on to queue for all the files in this folder
            for (TrieLeaf childLeaf : childFiles) {
                if (_root.getFullPath().endsWith("/")){
                    childFile = new DEFDocument(_root.getFullPath() + childLeaf.getName(), "FS:"+serviceName);
                } else {
                    childFile = new DEFDocument(_root.getFullPath() + "/" + childLeaf.getName(), "FS:"+serviceName);
                }
                childFile.setAction(ACTION_DELETE);
                postMessageToVector(childFile.toJson());
            }

        } catch (ConcurrentModificationException cme){
            logger.warn("Problem while iterating over children of " + _root.getFullPath() + " - could not delete all file nodes under this crate node");
        }

        // Now push all the sub-folders under this folder into this method to delete their files
        for (TrieBranch childNode : _root.getChildren()){
            deleteFolderAndChildren(childNode);
        }
    }




    /**
     * Check a single node in a crate (ie a folder) : it may been processed previously so this is a valid refresh-time action
     * Look for all of the files/leaves immediately under this folder to see if they still exist, have been deleted, or have been changed since the node was last checked
     * @param _trieNode a node in a Trie
     * @param _auth Ntlm Password Auth for Smb Share
     * @throws MQException MQ exception thrown if we cannot push to a queue
     * @throws JMSException JMS exception throw if we cannot push to a queue
     */
    protected void checkCrateFolderForChildFiles(TrieBranch _trieNode, NtlmPasswordAuthentication _auth) throws MQException, JMSException{

        if ( serviceController.isInstanceShutdown(serviceName)){
            logger.trace("in checkFolderForChildFiles but this service has been signalled to stop");
            return;
        }

        // Lets get a DEFFileDocument for this folder so we can look for any files in it
        boolean isSmb = false;
        if (_auth != null && _trieNode.getFullPath().startsWith("smb")){
            isSmb = true;
        }

        DEFFileDocument parent = null;
        String path = _trieNode.getFullPath();
        if (path.startsWith("file://")){
            path = path.substring(7);
        }
        // If the path is for Windows, it should start with a drive letter, eg C:  - if it's linux, it needs a leading "/" to be inserted cos a Trie don't know the difference
        if (! (path.indexOf(":")>0) && ! path.startsWith("/")  ){
            path = "/" + path;
        }
        if (path.endsWith("/")){
            path = path.substring(0,path.length()-1);
        }

        if (isSmb){
            try {

                parent = new DEFFileDocument(new SmbFile(path, auth),source, smb_domain,smb_username,smb_password);

            } catch (MalformedURLException murle) {
                logger.warn("FS Walker : exception opening SMB file at " + _trieNode.getFullPath() + " : " + murle.getLocalizedMessage());
            } catch (MissingFileException mfe){
                logger.info("Folder no longer exists " + _trieNode.getFullPath());
            } catch (BadDocumentException bdfe){
                logger.warn("Exception creating DEF File from " + _trieNode.getFullPath() +  ":: " + bdfe.getLocalizedMessage());
            }
        } else {
            try {
                parent = new DEFFileDocument(new File(path),source);
            } catch (MissingFileException mfe){
                logger.info("Folder no longer exists " + _trieNode.getFullPath());
            } catch (BadDocumentException bdfe){
                logger.warn("Unable to create new DEFFileDocument for " + _trieNode.getFullPath());
            }
        }

        if (parent == null){
            return;
        }

        // The folder exists - lets look for any child files it has now, and compare with the crate from last time round
        Map<String,List<String>> children = null;
        try {
            children = whatAboutTheChildren(Arrays.asList(parent.listFiles()), new ArrayList<>(_trieNode.getFiles()));
        } catch (BadDocumentException bdde){
            logger.warn("problem while listing all the child files of folder, " + parent.getAbsolutePath() + " : might be signal to stop");
            DEFLogManager.LogStackTrace(logger,"FileSysWalker:WATC",bdde);
        }

        // Now process them in order - INSERTS first
        if (null != children) {
            for (String s : children.get(DOCUMENT_CONSTANTS.ACTION_INSERT)) {
                String insertStr = s;
                String absPath = null;

                if (insertStr.indexOf("::")>0){
                    absPath = insertStr.substring(0,insertStr.indexOf("::"));
                }

                if (null != absPath ){
                    try {
                        DEFDocument updateDoc;
                        if (isSmb) {
                            // create SmbFile for this child file
                            // TODO - ******** full path?
                            updateDoc = new DEFFileDocument(absPath,source,smb_domain,smb_username,smb_password);
                        } else {
                            File f = new File(absPath);
                            updateDoc = new DEFFileDocument(f,source);
                        }
                        updateDoc.setAction(DOCUMENT_CONSTANTS.ACTION_INSERT);
                        // push doc on to the queue
                        postMessageToVector(updateDoc.toJson());
                        insertsLog.info(updateDoc.getId()+":"+updateDoc.getLastModifiedZulu()+":"+updateDoc.getDataSize());
                        trieNodeLeavesToInsert.add(updateDoc.getId()+"_:_"+updateDoc.getLastModified()+"_:_"+updateDoc.getDataSize()); // file to add to the crate
                    } catch (MissingFileException mfe) {
                        logger.warn("Folder, " + parent.getAbsolutePath() + " had a child file, " + s + " but now the file cannot be found");
                    } catch (BadDocumentException bdfe) {
                        logger.warn("Unable to create DEFFileDocument from child file of " + _trieNode.getFullPath() + File.separator + s);
                        DEFLogManager.LogStackTrace(logger, "FileSysWalker checkCrateFolderForChildFiles", bdfe);
                    }
                } else {
                    logger.warn("Problem parsing the string returned from WATC for Insert : string is " + s);
                }
            }

            // Now get the UPDATES
            for (String s : children.get(DOCUMENT_CONSTANTS.ACTION_UPDATE)) {
                logger.trace("Processing a child file with ACTION = UPDATE : " + s);
                try {
                    DEFDocument updateDoc;
                    if (isSmb) {
                        // create SmbFile for this child file
                        updateDoc = new DEFFileDocument(s, source,smb_domain,smb_username,smb_password);
                    } else {
                        File f = new File(s + File.separator);
                        trieNodeLeavesToInsert.add(f.getAbsolutePath()+"_:_"+f.lastModified()+"_:_"+f.length()); // file to add to the crate
                        updateDoc = new DEFFileDocument(f,source);
                    }
                    updateDoc.setAction(DOCUMENT_CONSTANTS.ACTION_UPDATE);
                    // push doc on to the queue
                    postMessageToVector(updateDoc.toJson());
                    logger.debug("Pushing doc to Vector for update : " + s);
                    updatesLog.info(updateDoc.getId()+":"+updateDoc.getLastModifiedZulu()+":"+updateDoc.getDataSize());
                } catch (MissingFileException mfe) {
                    logger.warn("Folder, " + parent.getAbsolutePath() + " had a child file, " + s + " but now the file cannot be found");
                } catch (BadDocumentException bdfe) {
                    logger.warn("Unable to create DEFFileDocument from child file of " + _trieNode.getFullPath() + File.separator + s);
                    DEFLogManager.LogStackTrace(logger, "FileSysWalker checkCrateFolderForChildFiles", bdfe);
                }
            }

            if (unchangedLog != null) {
                for (String s : children.get(WALKER_CONSTANTS.STATUS_UNCHANGED)) {
                    unchangedLog.debug(_trieNode.getFullPath() + "/" + s);
                }
            }

            for (String s : children.get(WALKER_CONSTANTS.STATUS_REJECT)) {
                logger.debug("Rejecting file, " + _trieNode.getFullPath() + s + " because it does not match all rules");
            }

            for (String s : children.get(ACTION_DELETE)) {
                DEFDocument deleteDoc = new DEFDocument(_trieNode.getFullPath() + File.separator + s, "FS:" + serviceName);
                deleteDoc.setAction(ACTION_DELETE);
                postMessageToVector(deleteDoc.toJson());
                trieNodesToDelete.add(deleteDoc.getId());
                deletesLog.debug(_trieNode.getFullPath() + "/" + s);
            }
        }
    }

    /**
     * Check a single node in a crate (ie a folder) : it has been processed previously so this is only valid as a refresh-time action
     * Need to check for folders that exist under this folder : look for inserts and deletes
     * @param _trieNode a node in Trie to start crawling from
     * @param _auth Ntlm SMB credentials for this CIFS share
     * @param _depth depth from crawl root for this node - controls how deep we can crawl
     * @throws MQException MQException MQ exception thrown if we cannot push to a queue
     * @throws JMSException JMS Exception MQ exception thrown if we cannot push to a queue
     */
    protected void checkCrateFolderForChildFolders(final TrieBranch _trieNode, NtlmPasswordAuthentication _auth, int _depth) throws MQException, JMSException{

        // did we get asked to stop already?
        if ( serviceController.isInstanceShutdown(serviceName)){
            logger.trace("in checkFolderForChildFolders for " + _trieNode.getName() + " but this service has been signalled to stop");
            return;
        }

//        TrieBranch branchNode = (TrieBranch)_trieNode;

        boolean isSmb = false;
        if (_auth != null && _trieNode.getFullPath().startsWith("smb")){
            isSmb = true;
        }

        String path = _trieNode.getFullPath();
        if (path.startsWith("file://")){
            path = path.substring(7);
        }
        // If the path is for Windows, it should start with a drive letter, eg C:  - if it's linux, it needs a leading "/" to be inserted
        if (! (path.indexOf(":")>0) && ! path.startsWith("/")  ){
            path = "/" + path;
        }
        if (path.endsWith("/")){
            path = path.substring(0,path.length()-1);
        }

        DEFFileDocument f = null;

        try {
            if (isSmb) {
                f = new DEFFileDocument(_trieNode.getFullPath(),source, smb_domain,smb_username,smb_password);
            } else {
                f = new DEFFileDocument(new File(path),source);
            }
        }catch (MissingFileException mfe){
            logger.warn("Folder not found while looking for child folders under it - " + path);
        } catch (BadDocumentException bdfe){
            logger.warn("Exception creating DEF File from " + path +  ":: " + bdfe.getLocalizedMessage());
        }

        logger.debug( " looking for child folders under " + path);

        // confirm whether there is a folder at the location identified by the node in the Trie

        if (f == null){
            logger.warn("Attempting to check if folder in a crate still exists but new DEFFileDocument is null. Maybe SMB credentials wrong?");
            return;
        } else if (! f.exists()){
            // There WAS a folder here - now its gone. Delete it from the index and the crates
            logger.info("Folder, " + path + " is missing! Deleting from crate...");
            deleteFolderAndChildren(_trieNode); // push to DELETE queue so we take stuff out of Solr
//            branchNode.removeNodeAndChildren(); // remove it from the crates
        } else if (f.isDir()){ // it still exists and is a folder

            logger.trace("Sending a branch node / folder to check on its leaf nodes/files. Node name is " + _trieNode.getName() + " - full path is " + _trieNode.getFullPath());
            // First look at the files that exist in this folder - compare with the Trie and make sure inserts/updates/deletes are put on the MQ
            checkCrateFolderForChildFiles(_trieNode,_auth); // check its immediate files - updates, inserts, deletes, touches

            // Now look at any child folders underneath the current folder
            DEFFileDocument[] subFolders = null;
            try {
                subFolders = f.listFolders(); // now look for sub folders
            } catch (BadDocumentException bdfe){
                logger.warn("Error while reading sub-folders under a folder : " + f.getAbsolutePath() + " :: " + bdfe.getLocalizedMessage());
            }
            if (subFolders != null) {
                for (DEFFileDocument subfolder : subFolders) {
                    if (subfolder.isDir() && filterFile(subfolder.getAbsolutePath())) {
                        // does this folder exist in the crates or is it new?
                        if (_trieNode.childExists(subfolder.getName())) { // the sub folder is in the crate : crawl it recursively with this function
                            checkCrateFolderForChildFolders(_trieNode.moveToChildNode(subfolder.getName()), _auth, _depth++);
                        } else {
                            // its a new folder
                            crawlNewFolder(subfolder, _depth++);
                        }
                    }
                }
            }

            // we checked all the folders that exist on the FS - now how about the ones in the crates - any been deleted?
            for (TrieBranch childNode : _trieNode.getChildren()){
                boolean folderExists = false;
                for (DEFFileDocument smbf : subFolders){
                    if (smbf.isDir() && smbf.getName().equals(childNode.getName()) && filterFile(smbf.getAbsolutePath())){
                        folderExists = true;
                        break;
                    }
                }
                if (! folderExists){
                    // there was a folder here previously but now its gone
                    deleteFolderAndChildren(childNode); // push entries on DELETE queue so files are removed from Solr
                }
            }
        } else {
            // f is not a directory - its just a regular file
            // Last  time we looked here, there was a folder at this point in the file-sys and we stored it in the crate
            // Now it is not a folder, there is a regular file at this point
            deleteFolderAndChildren(_trieNode);
            if (filterFile(_trieNode.getFullPath())){
                    f.setAction(DOCUMENT_CONSTANTS.ACTION_INSERT);
                    f.setTitle(_trieNode.getName());
                    postMessageToVector(f.toJson());
                    trieNodeLeavesToInsert.add(f.getName()+"_:_"+f.getLastModified()+"_:_"+f.getDataSize());
                    logger.debug("Previously this was a folder - now it is a file - " + f.getName());
                    insertsLog.info(f.getName()+":"+f.getLastModifiedZulu()+":"+f.getDataSize());
            } else {
                logger.debug("Last time, this was a folder. Now it is a file. However it doesn't meet the rules so it is skipped - " + f.getAbsolutePath());
            }
        }

    }

    /**
     * Compare the TrieLeaf nodes that we saw last time we looked at this folder against the current contents of the folder
     *
     * @param childFiles List of DEFFileDocuments that are all children of single parent folder
     * @param childNodes list of TrieLeaf nodes representing files that are stored in the crate
     * @return Map of four Lists - each List contains file names that children of a single parent folder. Maps are called "insert","delete","unchange","update"
     */
    protected Map<String,List<String>> whatAboutTheChildren(List<DEFFileDocument> childFiles, List<TrieLeaf> childNodes){


        if (childFiles == null || childNodes == null){
            logger.warn("whatAboutTheChildren received a null object to process");
            return new HashMap<String,List<String>>();
        }

        // Some temp storage units for each file depending on its status
        Map<String,List<String>> currentStatus = new HashMap<>();
        List<String>insert = new ArrayList<>(); // need file name, lastmod date, size - separate using "::" char
        List<String>update = new ArrayList<>();
        List<String>unchange = new ArrayList<>();
        List<String>delete = new ArrayList<>();
        List<String>rejects = new ArrayList<>();
        currentStatus.put(DOCUMENT_CONSTANTS.ACTION_INSERT,insert);
        currentStatus.put(DOCUMENT_CONSTANTS.ACTION_UPDATE,update);
        currentStatus.put(WALKER_CONSTANTS.STATUS_UNCHANGED,unchange);
        currentStatus.put(DOCUMENT_CONSTANTS.ACTION_DELETE,delete);
        currentStatus.put(WALKER_CONSTANTS.STATUS_REJECT, rejects);


        boolean filefound = false;
        // First look at all the files that exist on the file-sys underneath this folder
        for (DEFFileDocument defFile : childFiles){
            logger.trace("WATC: checking a childFile DEFFileDocument with id => " + defFile.getId() + " GUID => " + defFile.getGuid());
            if (defFile.isFile()) {  // the array contains folders and files - we only compare the files with TrieLeaves
                logger.trace("DEFFILE " + defFile.getId() + " says it is a file");
                filefound = false; // marker for each child file in the folder
                for (TrieLeaf leaf : childNodes) {
                    if (leaf.getName().equals(defFile.getName()) && filterFile(defFile.getName())){
                        // File on the file-system corresponds to a leaf in the trie so we have seen this one before
                        logger.trace("WATC - this file exists in the trie");
                        filefound = true;
                        if ( leaf.getLastModifiedDate()/1000 < defFile.getLastModified()/1000) {  // ignore milliseconds
                            update.add(defFile.getId());
                            logger.trace("WATC: UPDATE : leaf file " + leaf.getName() + " has lastmod = " + leaf.getLastModifiedDate() + " while deffile " + defFile.getId() + " has lastmod " + defFile.getLastModified() + " :: PUSH TO UPDATE");
                        } else {
                            unchange.add(defFile.getId());
                            logger.trace("WATC: UNCHANGED : " + defFile.getId());
                        }
                        break;
                    }
                }
                if (!filefound) {
                    // Its a new file
                    logger.debug("WATC : DEF File " + defFile.getId() + " does not exist in Trie so add it to INSERTS as a new file");
                    insert.add(defFile.getAbsolutePath() + "::" + defFile.getLastModified() + "::" + defFile.getDataSize());
                }
            } else {
                logger.trace("WATC: DEFFILE " + defFile.getId() + " says it is a FOLDER so do nothing");
            }
        }

        // Now check for deletes
        for (TrieLeaf leaf : childNodes){
            filefound = false;
            for (DEFFileDocument defFile : childFiles){
                if (leaf.getName().equals(defFile.getName())){
                    filefound = true;
                    break;
                }
            }
            if (filefound == false){
                // file no longer exists in folder
                delete.add(leaf.getName());
            }
        }
        return currentStatus;
    }


    /**
     * Crawler has found a new folder, not already seen in the crates.
     * Crawl its child files/folders, and sub-folders down three levels
     * Any folders found at three levels deep beneath this start are put back on a crawl queue and will be picked up by another worker thread

     * @param _rootfolder a folder (probably) to start crawling from
     * @param _level the depth of this starting folder from a "root", the top level folder for us to crawl from
     * @throws MQException exception throw if we are unable to add a new entry onto a MQ crawl queue
     * @throws JMSException exception throw if we are unable to add a new entry onto a MQ crawl queue
     */
    protected void crawlNewFolder(DEFFileDocument _rootfolder, int _level) throws MQException, JMSException{

        if (serviceController.isInstanceShutdown(serviceName)) {
            logger.trace("in new folders under " + _rootfolder.getPath() + "but this service has been signalled to stop");
            return;
        }

        logger.trace("Crawling a new folder - " + _rootfolder.getAbsolutePath());

        try {
            // For this run of the crawler, we iterate over three levels of folder
            // If there are child folders below 3 levels, put them on the queue and another worker thread will do them
            if (_rootfolder.isDir()) {
                for (DEFFileDocument child : _rootfolder.listFiles()) {
                    logger.trace("crawler found child  - " + _rootfolder.getName() + "/" + child.getName());
                    if (child.canRead() && filterFile(child.getPath())) {
                        if (child.isDir()) { // its a folder, put it on the input queue
                            logger.trace("Child is a folder");
                            if (serviceController.isInstanceShutdown(serviceName)) {
                                serviceController.setMessageForInstanceThread(serviceName, threadNumber, "stopping - just one more folder... " + _rootfolder.getName() + "/" + child.getName());
                            } else {
                                serviceController.setMessageForInstanceThread(serviceName, threadNumber, "processing folder " + _rootfolder.getName() + "/" + child.getName());
                            }
                            trieNodeBranchToInsert.add(child.getAbsolutePath());

                            for (DEFFileDocument grandChild : child.listFiles()) {
                                logger.trace("crawler found grandchild  - " + _rootfolder.getName() + "/" + child.getName() + "/" + grandChild.getName());
                                if (grandChild.canRead() && filterFile(grandChild.getPath())) {
                                    if (grandChild.isDir()) {
                                        logger.trace("Grandchild is a folder");
                                        // Its a folder - we don't process it, we just put it back on the crawl queue so it gets processed later
                                        crawlQueueSession.postTextMessage(getFolderAsJson(grandChild.getAbsolutePath(), _level + 2));
                                        trieNodeBranchToInsert.add(grandChild.getAbsolutePath());
                                        logger.debug("Found new folder to enqueue - " + grandChild.getAbsolutePath());
                                    } else if (filterFile(grandChild.getName()) && grandChild.getDataSize() < maxFileSize) {
                                        // Its a file and we can INSERT it
                                        logger.trace("grandchild is a file and we can insert it - " + grandChild.getName());
                                        grandChild.setAction(DOCUMENT_CONSTANTS.ACTION_INSERT);
                                        grandChild.setTitle(grandChild.getName());
                                        postMessageToVector(grandChild.toJson());
                                         insertsLog.info(grandChild.getAbsolutePath()+":"+grandChild.getLastModifiedZulu()+":"+grandChild.getDataSize());
                                         trieNodeLeavesToInsert.add(grandChild.getAbsolutePath()+"_:_"+grandChild.getLastModified()+"_:_"+grandChild.getDataSize());
                                    } else {
                                        //Its a file and we cannot insert it
                                        logger.debug("Rejecting file, " + grandChild.getAbsolutePath() + " because it does not match all rules");
                                    }
                                }
                            }

                        } else if (filterFile(child.getName()) && child.getDataSize() < maxFileSize) {
                            // Its a file and we can INSERT it
                            logger.trace("child is a file and we can insert it - " + child.getName());
                            child.setTitle(child.getName());
                            child.setAction(DOCUMENT_CONSTANTS.ACTION_INSERT);
                            postMessageToVector(child.toJson());
                            insertsLog.info(child.getAbsolutePath()+":"+child.getLastModifiedZulu()+":"+child.getDataSize());
                            trieNodeLeavesToInsert.add(child.getAbsolutePath()+"_:_"+child.getLastModified()+"_:_"+child.getDataSize());
                        } else {
                            //Its a file and we cannot insert it
                            logger.debug("Rejecting file, " + child.getAbsolutePath() + " because it does not match all rules");
                        }

                    } // file excluded by include rules. already been logged so nothing to do here
                }
            } else if (_rootfolder.isFile()) {
                // a file has been put on the crawler queue - it must be been added as a crawl "root" - not permitted
                // We can't have a file as a root because we don't know where to put it in the crate
                logger.warn("Invalid root - " + _rootfolder.getAbsolutePath() + " is a file so we cannot treat as a crawl root folder");
            }
        }catch (BadDocumentException bdfe){
            logger.warn("Error reading a folder or file while crawling subfolders of " + _rootfolder.getAbsolutePath() + bdfe.getLocalizedMessage());
            DEFLogManager.LogStackTrace(logger, "FileSysWalker checkCrateFolderForChildFolders", bdfe);
        }
    }

    private String getFolderAsJson(String _fname, int _depth){
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put(DOCUMENT_CONSTANTS.FIELDNAME_ID,_fname);
        node.put(WALKER_CONSTANTS.LEVEL,_depth);
        return node.toString();
    }


    private void postMessageToVector(String _message) throws JMSException{

        boolean failedToPost = false;
        try {
            vectorQueueSession.postTextMessage(_message);
        } catch (MQException mqe){
            failedToPost = true;
        }

        if (failedToPost){
            try {
                vectorQueueSession.close();
                vectorQueueSession = null;
                vectorQueueSession = vectorQueue.getSession(serviceName);
                vectorQueueSession.postTextMessage(_message);

            } catch (MQException mqe){
                logger.warn("Unable to post message to Vector queue : " + mqe.getLocalizedMessage());
            }
        }




    }


}
