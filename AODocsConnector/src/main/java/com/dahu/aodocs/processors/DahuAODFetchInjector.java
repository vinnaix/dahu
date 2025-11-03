package com.dahu.aodocs.processors;


import com.dahu.core.abstractcomponent.AbstractProcessor;
import com.dahu.core.exception.BadMetaException;
import com.dahu.core.interfaces.iDocument;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.types.Component;
import com.dahu.google.GoogleDocFetchAPI;
import com.dahu.plugins.core.auth.Google.GoogleCredentialsFactory;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.logging.log4j.Level;

import java.io.File;
import java.io.IOException;

import static com.dahu.aodocs.AODOCS_CONSTANTS.CONFIG_AODOCS_STORAGE;
import static com.dahu.aodocs.AODOCS_CONSTANTS.CONFIG_GOOGLESERVICE_JSONFILE_PATH;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 07/08/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 *
 * Vector Processor Stage
 *
 * AODocs Connector creates a new iDoc based on metadata only, as retrieved from AODocs via its API calls.
 * This stage will stream the original document from GDrive and put the raw bytes into the iDoc data field.
 * This Processor delegates streaming the document to the GoogleDocFetchAPI service.
 *
 * In order to stream the document from Google Drive, we need credentials that have permission to stream the document.
 * The AODocs Storage account is one account that we can be certain has the correct permission.
 * This account is defined in the AODocs Connector config for each library, but in Vector, we do not have access to that
 * config, and we do not want to define the name of the storage account in two separate config files.
 * Hence, the AODocs Connector writes the value of the AODocs storage account name as a field in the iDoc.
 * THe field name is defined in AODOCS_CONSTANTS.CONFIG_AODOCS_STORAGE
 * Given the account name, we can take the GoogleCredentials object that was generated from the Service Account and
 * create a delegated set of GoogleCredentials. Ideally we only want to do this once, not once per document.
 * Hence, when the first document is being processed, if the delegated credentials object is undefined, it reads the
 * storage account from the iDoc fields, and requests a new set of GoogleCredentials from the GoogleCredentialsFactory
 * and saves the value for future use.
 *
 */

public class DahuAODFetchInjector extends AbstractProcessor {

    private String service_json_path;
    GoogleCredentials serviceCredential;
    GoogleCredentials delegatedCredential = null;
    private static final Object myLock = new Object();

    public DahuAODFetchInjector(Level _level, Component _component) throws BadConfigurationException {
        super(_level, _component);

        if (null != serviceCredential){
            try {
                serviceCredential.refresh();
            } catch (IOException ioe){
                logger.warn("Unable to refresh Google Credentials Token : " + ioe.getLocalizedMessage());
                DEFLogManager.LogStackTrace(logger, "AODocsFetchInjector",ioe);
            }
        }
    }

    @Override
    public iDocument process(iDocument _iDoc){


        if (null != _iDoc.getId() && null != _iDoc.getMimeType()) {
            logger.debug("AODocFetcher : fetching Google doc for " + _iDoc.getId() + " : title: " + _iDoc.getTitle());
            if (null != serviceCredential) {
                if (null == delegatedCredential){ // no delegated credentials so this must be the first document to be processed
                    synchronized (myLock) { // Only want to do this once
                        if (null == delegatedCredential) { // check again that another thread didn't set the credentials while we were waiting on the synch
                            try {
                                String aoDocsStorageAccount = _iDoc.getFieldValue(CONFIG_AODOCS_STORAGE); // get account name from iDoc field (originally set by AODocs Connector
                                _iDoc.removeField(CONFIG_AODOCS_STORAGE); // We don't want the account name to propagate forward to SOlr
                                delegatedCredential = GoogleCredentialsFactory.getDelegatedCredentials(serviceCredential, aoDocsStorageAccount); // get new delegated account credentials
                            } catch (BadMetaException bme) {
                                logger.warn("Error reading AoDocsStorageAccount from meta, metas_" + CONFIG_AODOCS_STORAGE);
                                DEFLogManager.LogStackTrace(logger, "AOFetchInjector", bme);
                            } catch (BadConfigurationException bce) {
                                logger.warn("Error reading AoDocsStorageAccount from meta, metas_" + CONFIG_AODOCS_STORAGE);
                                DEFLogManager.LogStackTrace(logger, "AOFetchInjector", bce);
                            }
                        }
                    }
                } else {
                    // Delegated credentials already exists - still need to strip out the account name from the iDoc fields
                    _iDoc.removeField(CONFIG_AODOCS_STORAGE);
                }

                if (null != delegatedCredential){
                    // Get the raw bytes
                    byte[] data = new GoogleDocFetchAPI(delegatedCredential).fetch(_iDoc.getId(), _iDoc.getMimeType());

                    if (null != data && data.length > 0) {
                        _iDoc.setData(data);
                        // if we had to convert the doc type during export, update the mime type with the new mime type
                        if (_iDoc.getMimeType().equals("application/vnd.google-apps.document")){
                            String exportMimeType = GoogleDocFetchAPI.convertToSupportedExportFormat(_iDoc.getMimeType());
                            _iDoc.setMimeType(exportMimeType);
                        }
                    }
                } else {
                    logger.warn("Unable to fetch document body from Google Drive API : delegated credentials not found");
                }

            }
        }

        return _iDoc;
    }

    public boolean initialiseMe() throws BadConfigurationException {


        if (getIsPropertiesSet()){
            service_json_path = properties.get(CONFIG_GOOGLESERVICE_JSONFILE_PATH);

            File f = new File(service_json_path);
            if (! f.exists()){
                throw new BadConfigurationException("Json file not found at " + service_json_path);
            }
        }

        serviceCredential = GoogleCredentialsFactory.createCredential(service_json_path);
        if (null != serviceCredential) {
            return true;
        } else {
            return false;
        }
    }


}
