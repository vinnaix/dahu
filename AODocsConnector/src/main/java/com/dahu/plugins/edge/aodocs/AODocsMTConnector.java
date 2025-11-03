package com.dahu.plugins.edge.aodocs;

import com.dahu.aodocs.AODOCS_CONSTANTS;
import com.dahu.aodocs.APIservices.documentIdAPI;
import com.dahu.aodocs.APIservices.searchAPI;
import com.dahu.aodocs.types.AODComment;
import com.dahu.aodocs.types.AODLibrary;
import com.dahu.aodocs.types.AODdocumenttype;
import com.dahu.aodocs.types.DahuAODocsDocument;
import com.dahu.core.document.DOCUMENT_CONSTANTS;
import com.dahu.def.annotations.DEFAnnotationMT;
import com.dahu.def.config.PluginConfig;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.plugins.ListeningService;
import com.dahu.def.types.Queue2_0;
import com.dahu.def.types.Service;
import com.dahu.plugins.core.auth.Google.GoogleCredentialsFactory;
import com.dahu.plugins.edge.aodocs.storage.CSVDiskStore;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.logging.log4j.Level;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 06/08/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 * Connector to AODocs Google Drive repository
 *
 * Currently based on a Google Drive API token
 *
 * Need to provide following parameters
 *  Google API token
 *  EITHER  a Google Drive Library ID OR a Google Drive Library NAME
 *
 * The connector uses a per-library High Water Mark (timestemp) which is saved to disk
 * Only documents with a more recent last-mod timestamp that the HWM timeestamp will be included when the connector runs.
 * If the HWM file does not exist, or is set to 0, then all documents in the given library are retrieved by the Connector.
 * The format of the HWM file is this :
 *
 * libraryID1:timestamp(in milliseconds)
 * libraryID2:timestamp(in milliseconds)
 * libraryID3:timestamp(in milliseconds)
 *
 * The Connector is Multi-threaded in that multiple threads can run simultaneously, where each thread is processing
 * a single AODocs library.
 * On start-up, the service reads all of the libraries defined in the config and pushes them onto a work queue.
 * This is done only once per service-start, so the libraries will only be pushed on to the queue once.
 * Each thread listens on the internal work queue  and retrieves a library to process.
 * The thread will iterate over all Document Classes in the Library and retrieve all documents per class
 * Each document will be found in AODocs using the AODocs Search API to retrieve metadata, categories, folders, permissions etc
 * The data is pushed onto an output queue for further processing in Vector.
 *
 * The Connector is linked to a Connector Refresh Event, AODocsConnectorRefreshEvent
 * Both the Connector and the Event read from the same config file, so it contains some properties that only
 * apply to the context of a Connector running as a Service or to an Event
 *
 * The Event runs periodically to push all libraries onto the internal work queue so they get picked up and run again
 **
 */

@DEFAnnotationMT(isMultiThreaded="true")
public class AODocsMTConnector extends ListeningService {

    private String AODocsSecurityCode = null;

    private GoogleCredentials credential = null; // If we are using OAuth model, these are the credentials
    private String pathToServiceAccountJsonFile = null; // if we are using OAuth model, this is the path to the Json file
    private boolean securityModelOAuth = false; // if true we are using OAuth and a ServiceAccount Json file - else if false, we are using an API code

    private searchAPI  search = null;

    private Queue2_0 vectorQueue;

    private static boolean seededInternalQueue = false;

    private CSVDiskStore store;

    public AODocsMTConnector(Level _level, Service _service, int _threadNum) throws  BadConfigurationException, Exception {
        super(_level, _service, _threadNum);

        // We use EITHER an API code OR a service account to handle authentication to Google and AODocs APIs
        AODocsSecurityCode = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(AODOCS_CONSTANTS.CONFIG_AODOCS_SECURITYCODE);
        pathToServiceAccountJsonFile = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(AODOCS_CONSTANTS.CONFIG_GOOGLESERVICE_JSONFILE_PATH);

        // Check which type of security are we working with - either service account or API code - they are mutually exclusive
        if (null != pathToServiceAccountJsonFile){
            File f = new File(pathToServiceAccountJsonFile);
            if (f.exists()){
                securityModelOAuth = true;
                credential = GoogleCredentialsFactory.createCredential(pathToServiceAccountJsonFile);
            } else {
                throw new BadConfigurationException("Service Account Json file missing - file not found at " + pathToServiceAccountJsonFile);
            }
        } else if (null != AODocsSecurityCode){
            securityModelOAuth = false;
        } else {
            throw new BadConfigurationException("No valid security credentials available in the config.");
        }

        // Set up a Queues to push docs to Vector
        vectorQueue = this.getFirstOutputQueue();

        // Configure the Store to save the high-water-mark data for each library.
        // The store creates ONE file which is saved to disk with the filename provided in config
        if (null != PluginConfig.getFirstStore(serviceName) && PluginConfig.getFirstStore(serviceName) instanceof CSVDiskStore) {
            store = (CSVDiskStore) PluginConfig.getFirstStore(serviceName);
        } else {
            throw new BadConfigurationException("Unable to create new Store for AODocsConnector");
        }

    }


    /**
     * Process a single AODocs library
     * @param _message an MQueue Text message containing a JSON data package defining one AODocs library
     */
    public void processMessage(String _message) {

            //Read and parse the AODocs library from JSON message
        ObjectMapper mapper = new ObjectMapper();
        JsonNode msgNode;
        try {
            msgNode = mapper.readTree(_message);
        } catch (IOException ioe) {
            logger.warn("Unable to read JSON from incoming text message. Message is \"" + _message + "\"");
            return; // nothing more to do with this message
        }

        // Instantiate an AODLibrary instance to encapsulate all of the information for this library
        AODLibrary library = new AODLibrary(msgNode);

        String AODocsLibraryId = library.getLibraryID();
        String AODocsStorage = library.getStorageAdmin();
        String AODocsLibraryName = library.getLibraryName();

        logger.info("Opening library, \"" + AODocsLibraryName + "\"");

        // Read the h-w-m - timestamp for the last time this library was processed
        long hwm = store.getHWM(AODocsLibraryId);


        // Need a documentIdAPI instance to talk to an AODocs DocumentID API to read and set "comments"
        documentIdAPI docAPI = null;

        try {
            docAPI = new documentIdAPI(credential, AODocsStorage);
        } catch (IOException ioe) {
            logger.warn("IOException trying to create new documentTypeAPI : cannot process library, " + library.getLibraryName());
            return;
        } catch (BadConfigurationException bce) {
            logger.warn("BadConfigurationException trying to create new documentTypeAPI : cannot process library, " + library.getLibraryName());
            return;
        }

        // Iterate over all of the document classes that are defined for this library
        for (AODdocumenttype docType : library.getDocTypes()) {
            logger.info("Processing library \"" + AODocsLibraryName+ "\", document class, \"" + docType.getDocTypeName()+"\"");

            // If Service gets shutdown signal, we don't want to wait until finished processing ALL docs from ALL documents classes in this library
            // Not normal for a ListeningService but in this case, each process cycle could be VERY long for a large library so need this extra step
            // If we do get shut down half-way through processing a library, we can only make sure we start again next time from the same
            // place in the library ie essential that we DO NOT UPDATE the store with a newer HWM than already existed when we started on this library
            if (serviceController.isInstanceShutdown(serviceName)) {
                logger.info("Getting next Document Class but received signal to stop...");
                setMessage("Stopping");
                store.save();
                return;
            }


            String docTypeId = docType.getDocTypeID();
            String docTypeName = docType.getDocTypeName();

            setMessage("Searching for documents of type \"" + docTypeName + "\" library");

            // use AODocs Search API to retrieve all documents in this library paged in sets of 25
            if (securityModelOAuth) {
                try {
                    search = new searchAPI(credential, AODocsStorage, AODocsLibraryId, docTypeId);
                } catch (IOException ioe) {
                    logger.warn("IOException - failed to refresh accesstoken from GoogleCredentials");
                    return;
                } catch (BadConfigurationException bce) {
                    logger.warn("BadConfigurationException trying to create new search API : cannot process library, " + library.getLibraryName());
                    return;
                }
            } else {
                search = new searchAPI(AODocsSecurityCode, AODocsLibraryId, docTypeId);
            }

            Date currentTime = new Date(); // get timestamp before running a query

            if (hwm == 0) {
                logger.debug("AODocs search API query to return ALL documents from library, " + AODocsLibraryName);
            } else { // just select documents that are more recent than the current timestamp
                logger.debug("AODocs search API query to return documents from library, " + AODocsLibraryName + " modified after HWM of " + hwm);
            }

            int count = 0; // counter for total number of results processed
            List<DahuAODocsDocument> results = new ArrayList<>(); // Container for handling the results

            // Now get first page of results
            String nextPageToken = search.getRecentResultsPage(null, results, hwm);


            count = results.size();
            logger.debug("Processing first page of results from " + AODocsLibraryName);
            for (DahuAODocsDocument doc : results) {
                doc.setStorageAccount(AODocsStorage);
                processOneDocument(doc, docAPI);
            }

            // If Service gets shutdown, we don't want to wait until finished processing ALL docs from ALL documents classes in this library
            // Worst case, we have to wait while one page of search results are processed ie 25 documents retrieved from AODocs library
            if (serviceController.isInstanceShutdown(serviceName)) {
                logger.info("Received first page of documents but then received signal to stop...");
                setMessage("Stopping");
                store.save();
                return;
            }
            // OK, finished pushing first page of results on to the queue - lets get another page
            while (null != nextPageToken) { // iteration while more results exist

                // If Service gets shutdown, we don't want to wait until finished processing ALL docs from ALL documents classes in this library
                // Worst case, we have to wait while one page of search results are processed ie 25 documents retrieved from AODocs library
                if (serviceController.isInstanceShutdown(serviceName)) {
                    logger.info("Paging through AODocs pages of documents but received signal to stop...");
                    setMessage("Stopping");
                    store.save();
                    return;
                }


                logger.debug("Processing a page of results from " + AODocsLibraryName);
                // there's more results out there
                results.clear();
                nextPageToken = search.getRecentResultsPage(nextPageToken, results, hwm);
                count = count + results.size();
                for (DahuAODocsDocument doc : results) {
                    processOneDocument(doc, docAPI);
                }
            }

            // save the time we ran this query - results are cached so even if it takes time to iterate over all items, still need this HWM
            store.save(AODocsLibraryId, currentTime.getTime());
            logger.debug("Query complete. " + count + " documents found.");
        }
    }


    /**
     * Process a single item retrieved from AODocs search API
     * A single document in AODocs might represent multiple documents in Solr if there are multiple Attachments
     * The outcome of processing one AODocs documents is to push one or more documents to output queue for Vector
     * @param _doc a DahuAODocsDocument instance encapsulating all of the data returned from  search API for a single item
     * @param _docIdAPI handle on a documentIdAPI instance for requesting data on COMMENTS from AODocs API
     */
    private void processOneDocument(DahuAODocsDocument _doc, documentIdAPI _docIdAPI){

            setMessage("pushing " + _doc.getFolderPathStr() + "/" + _doc.getTitle());

            _docIdAPI.readAndSetComments(_doc);

            // One DOCUMENT in AODocs can have multiple ATTACHMENTS. Each ATTACHMENT must be pushed to Solr as separate item
            for (DahuAODocsDocument.AODocsDocumentAttachment attachment : _doc.getAttachments()){

                // First read all of the metadata that comes from the parent document in AODocs eg id, title, domain, library
                ObjectNode node = _doc.getJson();
                node.put("title",_doc.getTitle());

                // Now look for the metadata that exists for this attachment
                node.put("id",attachment.getFileId());
                node.put("mime",attachment.getMimeType());
                node.put("metas_attachmentname",attachment.getName());
                node.put("url",attachment.getLink());
                node.put("metas_iconurl",attachment.getIconLink());

                node.put("source","AODOCS_"+_doc.getLibraryName());
                node.put("action",DOCUMENT_CONSTANTS.ACTION_UPDATE);
                // flatten the permissions and add as an array of Strings formed as RIGHT_TYPE:NAME
                ArrayNode permNode = node.putArray("metas_ao_permission");
                for (DahuAODocsDocument.AODocsPermission permission : _doc.getPermissions()){
                    if (permission.isReader()){
                        if (permission.getType().equalsIgnoreCase("GROUP")){
                            permNode.add("GG:"+permission.getValue());
                        } else if (permission.getType().equalsIgnoreCase("USER")){
                            permNode.add("GU:"+permission.getValue());
                        }
                    }
                }

                for (String k : _doc.getFields().keySet()){
                    node.put("metas_"+k.toLowerCase(),_doc.getFields().get(k));
                }

                for (DahuAODocsDocument.AODocsField field : _doc.getCustomFields()){
                    for (String fieldValue : field.getValues()){
                        node.put("metas_"+field.getFieldName().toLowerCase(),fieldValue);
                    }
                }

                for (DahuAODocsDocument.AODocsCategory cat : _doc.getCategories()){
                    for (String catValue : cat.getValues()){
                        node.put("metas_"+cat.getCategoryName().toLowerCase(),catValue);
                    }
                }

                // Comments need to be flattened into a structured field
                if (_doc.getComments().size() > 0) {
                    StringBuilder sb = new StringBuilder();
                    for (AODComment comment : _doc.getComments()) {
                        sb.append(comment.toString() + "|");
                    }
                    sb.setLength(sb.length() - 1);
                    node.put("metas_comments", sb.toString());
                }

                logger.debug("AODocs Connector pushing doc to queue : " + attachment.getFileId());
                logger.trace(node.toString());
                vectorQueue.postTextMessage(node.toString());

            }
    }


}
