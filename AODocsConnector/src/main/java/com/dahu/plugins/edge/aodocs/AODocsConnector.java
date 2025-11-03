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
import com.dahu.def.plugins.YieldingService;
import com.dahu.def.types.Queue2_0;
import com.dahu.def.types.Service;
import com.dahu.plugins.core.auth.Google.GoogleCredentialsFactory;
import com.dahu.plugins.edge.aodocs.storage.CSVDiskStore;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.logging.log4j.Level;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


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
 * The connector depends on a High Water Mark file, which is based on the name of the service, with extension, hwm, and which exists in the run/bin folder.
 * If a HWM file exists for the service, it is used to determine the last-modified date of the most recent Document retrieved from AODocs.
 * Only documents with a more recent last-mod timestamp that the HWM timeestamp will be included when the connector runs.
 * If the HWM file does not exist, or is set to 0, then all documents are retrieved by the Connector.
 *
 * When the Connector is registered (AT DEF Startup)
 * Connector first ensures it has an AODocs Library ID. If a Library name is provided, it uses the LibraryAPI to convert to an ID
 * Given a Library ID, the connector gets a list of all of the Document Types that exist in the library.
 *
 * At run time, the Connector iterates over all of the Document Types and uses the Search API to retrieve all of the documents
 * from the Library
 **
 */

@DEFAnnotationMT(isMultiThreaded="false")
public class AODocsConnector extends YieldingService {

    private String AODocsSecurityCode = null;

    private GoogleCredentials credential = null; // If we are using OAuth model, these are the credentials
    private String pathToServiceAccountJsonFile = null; // if we are using OAuth model, this is the path to the Json file
    private boolean securityModelOAuth = false; // if true we are using OAuth and a ServiceAccount Json file - else if false, we are using an API code

    private Set<AODLibrary> libraries = new HashSet<>();
    private searchAPI  search;

    protected Queue2_0 vectorQueue = null;

    private CSVDiskStore store;

    public AODocsConnector(Level _level, Service _service, int _threadNum)throws BadConfigurationException, Exception {
        super(_level, _service, _threadNum);

        AODocsSecurityCode = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(AODOCS_CONSTANTS.CONFIG_AODOCS_SECURITYCODE);
        pathToServiceAccountJsonFile = PluginConfig.getPluginProperties(_service.getName()).getPropertyByName(AODOCS_CONSTANTS.CONFIG_GOOGLESERVICE_JSONFILE_PATH);

        JsonNode librariesArray = PluginConfig.getPluginProperties(_service.getName()).getPropertiesAsJson("libraries");
        if (null != librariesArray && librariesArray.isArray()){
            for (int i = 0; i < ((ArrayNode)librariesArray).size(); i++){
                JsonNode libraryNode = librariesArray.get(i);
                AODLibrary library = new AODLibrary(libraryNode);
                libraries.add(library);
            }
        }


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

        // Set up a Queue to push docs to Vector
        vectorQueue = this.getFirstOutputQueue();

        if (null != PluginConfig.getFirstStore(serviceName) && PluginConfig.getFirstStore(serviceName) instanceof CSVDiskStore) {
            store = (CSVDiskStore) PluginConfig.getFirstStore(serviceName);
        } else {
            throw new BadConfigurationException("Unable to create new Store for AODocsConnector");
        }

    }

    @Override
    protected int doWorkThenYield() throws BadConfigurationException {

        searchAPI search;

        for (AODLibrary library : libraries) {

            if (this.shouldYield()) { // signalled to stop the service.
                logger.info("Signalled to stop...");
                setMessage("Stopping");
                store.save();
                return Service.SHUT_ME_DOWN;
            }


            String AODocsLibraryId = library.getLibraryID();
            String AODocsStorage = library.getStorageAdmin();
            String AODocsLibraryName = library.getLibraryName();

            long hwm = store.getHWM(AODocsLibraryId);
            documentIdAPI docAPI = null;

            try {
                docAPI = new documentIdAPI(credential, AODocsStorage);
            } catch (IOException ioe){
                logger.warn("IOException trying to create new documentTypeAPI ");
            }
            for (AODdocumenttype docType : library.getDocTypes()){

                if (this.shouldYield()) { // signalled to stop the service.
                    logger.info("Signalled to stop...");
                    setMessage("Stopping");
                    store.save();
                    return Service.SHUT_ME_DOWN;
                }


                String docTypeId = docType.getDocTypeID();
                String docTypeName = docType.getDocTypeName();

                setMessage("Searching for documents of type \"" + docTypeName + "\" library");
                if (securityModelOAuth) {
                    try {
                        search = new searchAPI(credential, AODocsStorage, AODocsLibraryId, docTypeId);
                    } catch (IOException ioe) {
                        logger.warn("IOException - failed to refresh accesstoken from GoogleCredentials");
                        return Service.SHUT_ME_DOWN;
                    }
                } else {
                    search = new searchAPI(AODocsSecurityCode, AODocsLibraryId, docTypeId);
                }

                Date currentTime = new Date(); // get timestamp before running a query

                if (hwm == 0) {
                    logger.debug("AODocs search API query to return ALL documents from library, " + AODocsLibraryName);
                } else { // just select documents that are more recent than the current timestamp
                    logger.debug("AODocs search API query to return documents fro library, " + AODocsLibraryName + " modified after HWM of " + hwm);
                }

                int count = 0; // counter for total number of results processed
                List<DahuAODocsDocument> results = new ArrayList<>(); // Container for handling the results

                // Now get first page of results
                String nextPageToken = search.getRecentResultsPage(null, results, hwm);


                count = results.size();
                logger.debug("Processing first page of results from " + AODocsLibraryName);
                for (DahuAODocsDocument doc : results) {
                    doc.setStorageAccount(AODocsStorage);
                    processOneDocument(doc,docAPI);
                }

                // OK, finished pushing first page of results on to the queue - lets get another page
                while (null != nextPageToken) { // iteration while more results exist
                    if (this.shouldYield()) { // signalled to stop the service.
                        logger.info("Signalled to stop...");
                        setMessage("Stopping");
                        // deliberately NOT updating the HWM file so these documents will be retrieved next time round
                        return Service.SHUT_ME_DOWN;
                    }
                    logger.debug("Processing a page of results from " + AODocsLibraryName);
                    // there's more results out there
                    results.clear();
                    nextPageToken = search.getRecentResultsPage(nextPageToken, results, hwm);
                    count = count + results.size();
                    for (DahuAODocsDocument doc : results) {
                        processOneDocument(doc,docAPI);
                    }
                }

                // save the time we ran this query - results are cached so even if it takes time to iterate over all items, still need this HWM
                store.save(AODocsLibraryId,currentTime.getTime());
                logger.debug("Query complete. " + count + " documents found.");

            }
        }

        return Service.SHUT_ME_DOWN;
    }


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
                node.put("action",DOCUMENT_CONSTANTS.ACTION_INSERT); // TODO what if its an update? Without a Crate, how would we know?

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


    /**
     * Read the HWM value from disk
     * @param _filename HWM file for this job
     * @return first Long value found in the file
     */
    protected long readHWM(String _filename){

        long hwm = 0;

        Path HWM_FilePath = Paths.get("./" + _filename);

        // so its a List, although we are only ever interested in the first line
        if (Files.exists(HWM_FilePath)) {
            try {

                BufferedReader br = new BufferedReader(new FileReader(HWM_FilePath.toFile()));
                String inputLine = br.readLine();
                br.close();
                if (null != inputLine){
                    try {
                        hwm = Long.parseLong(inputLine);
                    } catch (NumberFormatException nfe){
                        logger.warn("Invalid data in the HWM file for " + serviceName + " check the file at " + _filename + ". It should contain a long representing a timestamp since epoch");
                    }
                }

            } catch (IOException ioe) {
                logger.warn("Failed to open HWM file for " + serviceName + " :: " + ioe.getLocalizedMessage());
            }
        }
        return hwm;

    }

    /*
    * Write the HWM value to disk
     */
    protected synchronized  void saveHWM(String _filename, long _value){
    /*
        Write the HWM to disk
     */
        List<String> lines = Arrays.asList("" + _value);  // Java NIO wants to write a List of strings to file so give it one, with just one line in it
        Path file = Paths.get("./" + _filename);
        try {
            Files.write(file, lines, Charset.forName("UTF-8"));
        } catch (IOException ioe) {
            logger.warn("Cannot update HWM file, " + _filename + ".hwm" + " :: " + ioe.getLocalizedMessage());
        }
    }


}
