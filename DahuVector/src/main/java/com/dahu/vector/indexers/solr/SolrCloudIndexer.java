package com.dahu.vector.indexers.solr;

import com.dahu.core.abstractcomponent.AbstractTerminator;
import com.dahu.core.document.DEFFileDocument;
import com.dahu.core.document.DOCUMENT_CONSTANTS;
import com.dahu.core.exception.BadMetaException;
import com.dahu.core.interfaces.IndexType;
import com.dahu.core.interfaces.iDocument;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.core.utils.LogUtils;
import com.dahu.def.config.CONFIG_CONSTANTS;
import com.dahu.def.config.ServerConfig;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.types.Component;
import org.apache.logging.log4j.Level;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.dahu.vector.indexers.solr.SOLR_CONSTANTS.*;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 13/09/2017
 * copyright Dahu Ltd 2017
 * <p>
 * Changed by :
 *
 * Implementation of a DEFIndexerService that opens a connection to a SolrCloud and sends documents to be indexed or deleted
 *
 */

public class SolrCloudIndexer extends AbstractTerminator {

    private static final IndexType type = IndexType.SOLR;
    private String indexName;

    private static final String CONFIG_HOSTNAME = "solr_hostname";
    private static final String CONFIG_ZKPORT = "solr_zkport";
    private static final String CONFIG_INDEXNAME = "solr_index";
    private static final String CONFIG_LOGLEVEL = "loglevel";
    private static final String CONFIG_SOLR_TIMEOUT = "solr_TimeOut";
    private static final String CONFIG_SOLR_CONNECTION_TIMEOUT = "solr_connection_timeout";
    private static final String CONFIG_SOLR_SCHEMA_MAP_FILE = "solrSchemaMap";

    private String solrIndexerHostName = null;
    private String solrIndexerPort = null;
    private String solrIndexerHandler = "update";
    private int solrConnectionTimeout = 5000; // default 5 seconds for HTTP connection
    private int solrTimeout = 10000; // default 10 seconds for Solr to perform action
    private String solrSchemaMapFile = null; // path to a file that contains mapping data from iDoc to Solr Schema

    private ConcurrentUpdateSolrClient solr = null;

    private SolrFieldMapper mapper = null;

    private static boolean isCommittingFlag = false; // if true, a solr committer is committing so wait


    public SolrCloudIndexer(Level _level, Component _component){
        super(_level, _component);

    }

    protected static void setIsCommitting(boolean _isCommitting){
        SolrCloudIndexer.isCommittingFlag = _isCommitting;
    }

    public boolean initialiseMe() throws BadConfigurationException{
        if (getIsPropertiesSet()) {
            for (String k : properties.keySet()) {
                if (k.equalsIgnoreCase(CONFIG_HOSTNAME)) {
                    solrIndexerHostName = properties.get(k);
                } else if (k.equalsIgnoreCase(CONFIG_ZKPORT)) {
                    solrIndexerPort = properties.get(k);
                } else if (k.equalsIgnoreCase(CONFIG_LOGLEVEL)) {
                    DEFLogManager.changeLogLevel(logger, LogUtils.getLogLevel(properties.get(k)));
                } else if (k.equalsIgnoreCase(CONFIG_INDEXNAME)) {
                    indexName = properties.get(k);
                } else if (k.equalsIgnoreCase(CONFIG_SOLR_CONNECTION_TIMEOUT)){
                    String conTimeoutStr = properties.get(k);
                    try {
                        solrConnectionTimeout = Integer.parseInt(conTimeoutStr);
                    } catch (NumberFormatException nfe){
                        logger.warn("Bad config item for Solr Connection Timeout - expected a number, received " + conTimeoutStr + "  using default for Connection Timeout");
                    }
                } else if (k.equalsIgnoreCase(CONFIG_SOLR_TIMEOUT)){
                    String timeoutStr = properties.get(k);
                    try {
                        solrTimeout = Integer.parseInt(timeoutStr);
                    }catch (NumberFormatException nfe){
                        logger.warn("Bad config item for Solr Timeout - expected a number, received " + timeoutStr + "  using default for Solr Timeout");
                    }
                } else if (k.equalsIgnoreCase(CONFIG_SOLR_SCHEMA_MAP_FILE)){
                    String confDif = ServerConfig.getServerAttributes().getPropertyByName(CONFIG_CONSTANTS.CONFIGSETTINGSDIR);
                    solrSchemaMapFile = properties.get(k);
                    File f = new File(confDif+"/"+solrSchemaMapFile);
                    if (f.exists()){
                        try {
                            mapper = new SchemaDrivenSolrFieldMapper(confDif+"/"+solrSchemaMapFile);
                        }catch (IOException ioe){
                            logger.warn("Bad config - unable to open file, " + solrSchemaMapFile);
                            throw new BadConfigurationException("Unable to open config file, " + solrSchemaMapFile);
                        }
                    }
                }
            }

            if (null == mapper){
                mapper = new DefaultSolrFieldMapper();
            }

            if (solrIndexerHostName != null && solrIndexerPort != null && indexName != null) {
                solrConnect();
            }
            if (solr == null) {
                throw new BadConfigurationException("Unable to create new Solr client - check config for " + CONFIG_HOSTNAME + ", " + CONFIG_ZKPORT + " and " + CONFIG_INDEXNAME);
            } else {
                logger.info("Initialised SolrCloudIndexer : server = " + solrIndexerHostName + ":" + solrIndexerPort + " : index => " + indexName);
                return true;
            }
        } else {
            logger.warn("Cannot initialize SolrCloudndexer  no properties have been provided by client service");
            return false;
        }

    }

    /**
     * Process a document
     * Determine its action - then send as a request to SOLR
     * @param _iDoc  a document to be processed
     */
    @Override
    public boolean terminate(iDocument _iDoc) {

        if (solr == null){
            this.solrConnect();
        }

        if (solr == null){
            logger.warn("Unable to connect to solr at " + solrIndexerHostName);
            return false;
        }

        if (isCommittingFlag) {
            logger.trace("Pausing Solr indexing for a commit");
            while (isCommittingFlag) {
                try {
                    logger.trace("Paused Solr indexing for a commit - isCommittingFlag is " + isCommittingFlag);
                    Thread.sleep(500);
                } catch (InterruptedException ie) {

                }
            }
        }

        String status = null;
        if (_iDoc.getAction().equalsIgnoreCase(DOCUMENT_CONSTANTS.ACTION_DELETE)) {
            // put a message in the Push API Data Store for a client to see what happened
            status = deleteDoc(_iDoc.getId());
        } else if (_iDoc.getAction().equalsIgnoreCase(DOCUMENT_CONSTANTS.ACTION_UPDATE)) {
            // update the last mod
            // put a message in the Push API Data Store for a client to see what happened
            status = updateDoc(_iDoc);
        } else {
            // put a message in the Push API Data Store for a client to see what happened
            status = insertDoc(_iDoc);
        }
        logger.info("SolrCloudIndexer :: " + _iDoc.getAction() + " => " + _iDoc.getId() + " ==> " + solrIndexerHostName+":"+solrIndexerPort +"/solr/" + indexName + " => "  + status);
        setPushAPIResponseMessage(_iDoc.getId(),status);

        if (status.equals("success"))
            return true;
        else
            return false;

    }


    private String insertDoc(iDocument _iDoc) {
        if (this.solr != null) {

            logger.debug("Preparing insert of " + _iDoc.getId() + " to Solr");

            // create the Solr Doc we are going to send to index
            SolrInputDocument solrDoc = new SolrInputDocument();

            // Get all the field names that are valid for Solr
            solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_ID, _iDoc.getId());
//            solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_GUID, _iDoc.getGuid());


            if (_iDoc.getUrl() != null) {
                solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_URL, _iDoc.getUrl());
            }
            if (_iDoc.getTitle() != null) {
                solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_TITLE, _iDoc.getTitle());
            }
            if (_iDoc.getMimeType() != null) {
                solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_MIME, _iDoc.getMimeType());
            }
            if (_iDoc.getParentId() != null) {
                solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_PARENT_ID, _iDoc.getParentId());
            }
            solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_SIZE, _iDoc.getDataSize());
            if (_iDoc.getLastModifiedZulu() != null) {
                solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_LAST_MODIFIED_DATE_STR, _iDoc.getLastModifiedZulu());
                solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_LAST_MODIFIED_DATE, _iDoc.getLastModifiedZulu());
            }
            if (_iDoc.getSource() != null) {
                solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_SOURCE, _iDoc.getSource());
            }
            if (_iDoc.getChildIds().size() > 0) {
                StringBuilder sb = new StringBuilder();
                for (String s : _iDoc.getChildIds()) {
                    sb.append(s + ",");
                }
                sb.setLength(sb.length() - 1);
                solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_CHILD_IDS, sb.toString());
            }

            if (_iDoc instanceof DEFFileDocument){
                for (String s : ((DEFFileDocument)_iDoc).getAllowTokenShare()){
                    solrDoc.addField(SOLRFIELD_ALLOW_TOKEN_SHARE, s);
                }
                for (String s : ((DEFFileDocument)_iDoc).getAllowTokenParent()){
                    solrDoc.addField(SOLRFIELD_ALLOW_TOKEN_PARENT, s);
                }
                for (String s : ((DEFFileDocument)_iDoc).getAllowTokenDocument()){
                    solrDoc.addField(SOLRFIELD_ALLOW_TOKEN_DOCUMENT, s);
                }
                for (String s : ((DEFFileDocument)_iDoc).getDenyTokenShare()){
                    solrDoc.addField(SOLRFIELD_DENY_TOKEN_SHARE, s);
                }
                for (String s : ((DEFFileDocument)_iDoc).getDenyTokenParent()){
                    solrDoc.addField(SOLRFIELD_DENY_TOKEN_PARENT, s);
                }
                for (String s : ((DEFFileDocument)_iDoc).getDenyTokenDocument()){
                    solrDoc.addField(SOLRFIELD_DENY_TOKEN_DOCUMENT, s);
                }
                if (null != ((DEFFileDocument)_iDoc).getOwner() ){
                    solrDoc.addField(SOLRFIELD_OWNER, ((DEFFileDocument)_iDoc).getOwner());
                }
                if (null != ((DEFFileDocument)_iDoc).getServername() ){
                    solrDoc.addField(SOLRFIELD_SERVERNAME, ((DEFFileDocument)_iDoc).getServername());
                }
                if (null != ((DEFFileDocument)_iDoc).getSharename() ){
                    solrDoc.addField(SOLRFIELD_SHARENAME, ((DEFFileDocument)_iDoc).getSharename());
                }
                if (null != ((DEFFileDocument)_iDoc).getName() ){
                    solrDoc.addField(SOLRFIELD_FILENAME, ((DEFFileDocument)_iDoc).getName());
                }
                if (null != ((DEFFileDocument)_iDoc).getExtension() ){
                    solrDoc.addField(SOLRFIELD_EXT, ((DEFFileDocument)_iDoc).getExtension());
                }

            }


                // Iterate over every meta that exists for this doc - check if Solr is happy to accept them as a field
            for (String f : _iDoc.getFieldNames()) {
                try {
                    if (_iDoc.getFieldValue(f) != null && mapper.getSolrFieldName(f) != null && solrDoc.getFieldValue(f) == null) {
                        if (mapper.isMulti(f)) {
                            for (String value : _iDoc.getFieldValues(f)) {
                                solrDoc.addField(mapper.getSolrFieldName(f), value);
                            }
                        } else if (_iDoc.getFieldValues(f).size() > 1) {
                            StringBuilder sb = new StringBuilder();
                            for (String s : _iDoc.getFieldValues(f)) {
                                sb.append(s + ",");
                            }
                            sb.setLength(sb.length() - 1);
                            solrDoc.addField(mapper.getSolrFieldName(f), sb.toString());
                        } else {
                            solrDoc.addField(mapper.getSolrFieldName(f), _iDoc.getFieldValue(f));
                        }
                    } else if (_iDoc.getFieldValue(f) != null && mapper.isDynamicField(f)){
                        for (String value : _iDoc.getFieldValues(f)){
                            solrDoc.addField(f,value);
                        }
                    }
                } catch (BadMetaException bme) {
                        // do nothing - skip this meta and move on
//                        logger.trace("Unable to add meta from iDoc to solr Doc : " + bme.getLocalizedMessage());
                }
            }

                // Solr expects to get processed text not raw bytes
            if ((!_iDoc.isStub()) && _iDoc.getBody() != null) {
                solrDoc.addField("_text_", _iDoc.getBody());
            }

                // TODO do we care about the XHTML that is available?
             logger.debug("Sending doc, " + _iDoc.getId() + " to solr at " + solrIndexerHostName + ":" + solrIndexerPort);

            try {
                UpdateResponse response = solr.add(solrDoc);
                    // UpdateResponse returns code 0 for success or throws exception in all other cases, so no reason to read the code
                logger.debug("Solr response = " + response);
            } catch (IOException ioe) {
                logger.warn("IO Failure from Solr for doc " + _iDoc.getId() + " :: " + ioe.getLocalizedMessage());
//                ioe.printStackTrace();
                return "IOError";
            } catch (SolrServerException sse) {
                logger.warn("Solr Error for doc " + _iDoc.getId() + " :: " + sse.getLocalizedMessage());
//                sse.printStackTrace();
                return "SolrError";
            } catch (Exception e) {
                logger.warn("Error for doc " + _iDoc.getId() + " :: " + e.getLocalizedMessage());
//                e.printStackTrace();
                return "GeneralError";
            }

            return "success";
        } else {
            return "fail : Solrj client is not ready";
        }
    }


    private String updateDoc(iDocument _iDoc){

        if (null != solr ) {

            logger.debug("Preparing update of " + _iDoc.getId() + " to Solr");

            // create the Solr Doc we are going to send to index
            SolrInputDocument solrDoc = new SolrInputDocument();

            // Get all the field names that are valid for Solr
            solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_ID, _iDoc.getId());

            if (_iDoc.getLastModifiedZulu() != null) {
                Map<String, Object> fieldModifier = new HashMap<>();
                fieldModifier.put("set", _iDoc.getLastModifiedZulu());
                solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_LAST_MODIFIED_DATE_STR, fieldModifier);
                solrDoc.addField(SOLR_CONSTANTS.SOLRFIELD_LAST_MODIFIED_DATE, fieldModifier);
            }

            logger.debug("Sending doc, " + _iDoc.getId() + " to solr at " + solrIndexerHostName + ":" + solrIndexerPort);

            try {
                UpdateResponse response = solr.add(solrDoc);
                // UpdateResponse returns code 0 for success or throws exception in all other cases, so no reason to read the code
                logger.debug("Solr response = " + response);

            } catch (IOException ioe) {
                logger.warn("IO Failure from Solr for doc " + _iDoc.getId() + " :: " + ioe.getLocalizedMessage());
//            ioe.printStackTrace();
                return "IOError";
            } catch (SolrServerException sse) {
                logger.warn("Solr Error for doc " + _iDoc.getId() + " :: " + sse.getLocalizedMessage());
//            sse.printStackTrace();
                return "SolrError";
            } catch (Exception e) {
                logger.warn("Error for doc " + _iDoc.getId() + " :: " + e.getLocalizedMessage());
//            e.printStackTrace();
                return "GeneralError";
            }

            return "success";
        } else {
            return "fail : Solrj client is not ready";
        }
    }

    private String deleteDoc(String _id){
            if (this.solr != null) {
                    try {
                        String query = "id:\"" + _id + "\"";
                        UpdateResponse response = solr.deleteByQuery(query);
                        return "success";
                    } catch (IOException ioe) {
                        logger.warn("IO Failure from Solr for doc " + _id + " :: " + ioe.getLocalizedMessage());
                        return "IOError";
                    } catch (SolrServerException sse) {
                        logger.warn("Solr Error for doc " + _id + " :: " + sse.getLocalizedMessage());
                        return "SolrError";
                    }
            } else {
                logger.warn("Solr has not been initialized - failed to send this document to be deleted : " + _id);
                return "SolrIndexerError";
            }
    }


    public void stopIndexer() {
        if (solr != null){
                solr.close();
                // TODO IF we get IO Exception trying to close a connection, chances are it is already closed, but maybe need to check for some other failure?
        }
    }


    /**
     * Open connection to Solr through its ZK array (SolrCloud mode)
     */
    private void solrConnect(){

        String solrUrl = "http://" + solrIndexerHostName + ":" + solrIndexerPort + "/solr/" + indexName;
        logger.info("Configured SolrCloudIndexer : SolrURL => " + solrUrl);
        solr = new ConcurrentUpdateSolrClient.Builder(solrUrl).withQueueSize(10).withThreadCount(10).build();
    }

    public void optimise(){
        if (solr == null){
            this.solrConnect();
        }
        if (solr != null){
            try {
                solr.optimize();
            } catch (SolrServerException sse){
                logger.warn("Failed to optimise Solr - Server Error " + sse.getLocalizedMessage());
            } catch (IOException ioe){
                logger.warn("Failed to optimise Solr - IO Error " + ioe.getLocalizedMessage());
            }
        }
    }

}
