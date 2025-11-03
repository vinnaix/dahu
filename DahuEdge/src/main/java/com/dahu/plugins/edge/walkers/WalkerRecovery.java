package com.dahu.plugins.edge.walkers;

import com.dahu.core.document.DOCUMENT_CONSTANTS;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.core.trie.Trie;
import com.dahu.core.trie.TrieBranch;
import com.dahu.def.annotations.DEFAnnotationMT;
import com.dahu.def.config.PluginConfig;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.exception.MQException;
import com.dahu.def.plugins.YieldingService;
import com.dahu.def.types.MQueue;
import com.dahu.def.types.Service;
import com.dahu.vector.indexers.solr.SolrException;
import com.dahu.vector.indexers.solr.SolrStreamer;
import org.apache.logging.log4j.Level;
import org.apache.solr.common.util.Hash;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.jms.JMSException;
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 23/07/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 * Rebuild a broken Trie and re-populate a crawler Queue in the event one or both are corrupt.
 *
 * In the first implementation, data is retrieved from a Solr index, via a Solr Stream API, retrieving just the data necessary
 * to rebuild these two data structures. At a minimum, a Crate stores full path to folders and files, and stores
 * last-modified date (as a Long) and size (as an Int) for files.
 * All folders that are found in the Solr index are assumed to have been successfully processed.
 * They are pushed on to the crawl queue to be crawled again. However, the crawler will find that all of the files in these folders
 * have not changed so they are not pushed to Vector. However, all subfolders *are* pushed to be re-crawled.
 *
 *
 */

@DEFAnnotationMT(isMultiThreaded="false")
public class WalkerRecovery extends YieldingService {

    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

//    Trie crate = null;
    MQueue crawlQueue = null;
    MQueue.MQueueSession crawlQueueSession = null;

    String solrHostPort = null;
    String solrIndex = null;
    String triename = null;
    boolean includeTrie = false;

    String id;
    String lastMod;
    Set<String> idSet = new HashSet<>();


    public WalkerRecovery(Level _level, Service _service, int _threadNumber) throws BadConfigurationException, MQException {
        super(_level, _service, _threadNumber);

        this.solrHostPort = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(WALKER_CONSTANTS.CONFIG_RECOVERY_SOLR_HOSTNAMEPORT);
        this.solrIndex = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(WALKER_CONSTANTS.CONFIG_RECOVERY_INDEXNAME);
        this.triename = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(WALKER_CONSTANTS.CONFIG_RECOVERY_TRIENAME);


        crawlQueue = this.getFirstInputQueue();

        // Iterate over all the storage components until we find one that has the same name as the crate identified on our config
//         for (String name : PluginConfig.getStores().getAllComponentKeys()){
//             if (((Trie)PluginConfig.getFirstStore(name)).getName().equals(this.triename)){
//                 crate = (Trie)PluginConfig.getFirstStore(name);
//             }
//         }



        logger.warn("RECOVERY MODE :: SOURCE = SOLR");
        logger.warn("REBUILDING TRIE CRATE AND CRAWL QUEUE FROM DATA IN SOLR");
        logger.warn("SOLR INSTANCE => " + solrHostPort + " SOLR INDEX => " + solrIndex);


        // First lets clear down the queue
        logger.debug("Purging the crawl queue, " + crawlQueue.getQueueName());
        try {
            crawlQueue.purge();
        } catch (Exception e){
            logger.warn("Unable to purge queue ; " + e.getLocalizedMessage());
            DEFLogManager.LogStackTrace(logger, "WalkerRecovery",e);
            throw new BadConfigurationException("Unable to purge queue - cannot start service");
        }

//        logger.debug("Deleting Trie");
//        crate = crate.recreateTrie();

        // Create a new Queue Session to post messages to
        try {
            crawlQueueSession = crawlQueue.getSession(serviceName);
        } catch (JMSException jmse){
            logger.warn("Unable to get session for crawl Queue");
            DEFLogManager.LogStackTrace(logger,"WalkerRecover",jmse);
        }
        if (null == crawlQueueSession){
            throw new BadConfigurationException("Unable to create new MQueue Session for purged queue");
        }

        logger.debug("Finished setting up Recovery service.");
    }

    @Override
    protected int doWorkThenYield() throws BadConfigurationException {
        if (null == crawlQueueSession){
            return Service.SHUT_ME_DOWN;
        }
        SolrStreamer streamer = new SolrStreamer(solrHostPort, solrIndex);
        InputStream is = null;
        BufferedWriter writer = null;

        try {
            logger.debug("Opening Temp file at " + WALKER_CONSTANTS.RECOVERY_TEMPFILE_PREFIX + solrIndex);
            writer = new BufferedWriter(new FileWriter(WALKER_CONSTANTS.RECOVERY_TEMPFILE_PREFIX + solrIndex));
        } catch (IOException ioe){
            logger.warn("Unable to write Solr Stream to temp file, " + WALKER_CONSTANTS.RECOVERY_TEMPFILE_PREFIX + solrIndex + " error : " + ioe.getLocalizedMessage());
            return Service.SHUT_ME_DOWN;
        }

        try {
            is = streamer.stream("*:*", "id,last_modified,guid");
        } catch (SolrException se){
            logger.warn("Error getting data from Solr : " + se.getLocalizedMessage());
            return Service.SHUT_ME_DOWN;
        }
        try {
            if (null != is) {
                BufferedReader in = new BufferedReader(new InputStreamReader(is));
                String line;
                while ((line = in.readLine()) != null && line.indexOf("\"EOF\":true") <= 0) {
                    logger.trace("Solr:Stream =>  " + line);
                    if(processLine(line)){
                        writeTrieNode(writer);
//                        insertToTrie();
//                        logger.trace("Added entry to Trie. Now saving trie");
//                        crate.saveMe();
                    }
                }
                insertToQueue();

            }
        } catch(IOException ioe){
            logger.warn("Something bad happened while reading from Solr Stream");
            DEFLogManager.LogStackTrace(logger, "WalkerRecovery",ioe);
        }

        return Service.SHUT_ME_DOWN;
    }

    private void insertToQueue(){

        List<String> idSorted = idSet.stream().collect(Collectors.toList());
        Collections.sort(idSorted, Collections.reverseOrder());

        String lastFolder = null;
        String thisFolder = null;
        for (String id : idSorted){
            // if this folder is a parent of last folder, then don't insert it
            if (id.indexOf("/") > 0) {
                thisFolder = id;
                if (null == lastFolder) {
                    // do nothing - first line
                } else if (! lastFolder.startsWith(thisFolder)) {
                    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
                    node.put(DOCUMENT_CONSTANTS.FIELDNAME_ID,thisFolder+"/");
                    node.put(WALKER_CONSTANTS.LEVEL,99); // we don't know the crawl depth so lets just say its deep

                    try {
                        if (!crawlQueueSession.postTextMessage(node.toString())){
                            logger.warn("Posting TextMessage to queue failed");
                        } else {
                            logger.debug("Posted folder to crawl queue");
                        }
                    } catch (Exception e){
                        logger.warn("Unable to post to queue");
                        DEFLogManager.LogStackTrace(logger,"WalkerRecovery",e);
                    }
                }
                lastFolder = thisFolder;
            }
        }
    }

    private void writeTrieNode(BufferedWriter writer){

        // id, lastMod
        long lastModified = 0;
        try {
            Date lm = df.parse(lastMod);
            lastModified = lm.getTime();
        } catch (ParseException pe){
            DEFLogManager.LogStackTrace(logger,"WalkerRecovery",pe);
        }

        try {
            writer.write("{\"id\":\"" + id + "\",\"lastmod\":\"" + lastModified + "\"},\n");
        } catch (IOException ioe){
            logger.warn("Unable to write to temp file : " + ioe.getLocalizedMessage());
        }

    }


    /*
    private void insertToTrie(){
        // id, lastMod
        long lastModified = 0;
        try {
            Date lm = df.parse(lastMod);
            lastModified = lm.getTime();
        } catch (ParseException pe){
            DEFLogManager.LogStackTrace(logger,"WalkerRecovery",pe);
        }

        String smbFilePath = id.substring(0,id.lastIndexOf("/"));
        String filename = id.substring(id.lastIndexOf("/")+1);

        TrieBranch folder = crate.insert(smbFilePath);
        folder.addLeaf(filename, lastModified,0);
        logger.debug("Add to Trie : folder => " + folder.getFullPath() + " :: then added leaf " + filename);
    }

     */


    /*
    * Process a line of data returned from Solr
    * Need to build up a valid ID string and LastMod string for a records, then we can process that record
    * and start building up the next one
    * Returns TRUE if all data is present to process
    * else returns FALSE to continue processing more lines
     */
    private boolean processLine(String _line){
        String line = _line;
        boolean endOfBlock = false;

        if (line.endsWith(",")) {
            line = line.substring(0, line.length() - 1);
        }
        if (line.endsWith("}")) {
            line = line.substring(0, line.length() - 1);
            endOfBlock = true;
        }

        if (_line.indexOf("\"id\":") > 0){
            line = line.substring(line.indexOf("\"id\":")+5);
            id = line.replaceAll("\"","");
            if (null != id && id.indexOf("/") > 0) {
                idSet.add(id.substring(0,id.lastIndexOf("/")));
            }
        } else if (line.indexOf("last_modified")>0){
            line = line.substring(line.indexOf("\"last_modified\":")+16);
            lastMod = line.replaceAll("\"","");
        }

        if (endOfBlock && null != id && null != lastMod ){
            return true; // got all the data to process something
        } else {
            return false;
        }
    }


}
