package com.dahu.vector.processors;


import com.dahu.core.abstractcomponent.AbstractProcessor;
import com.dahu.core.interfaces.iDocument;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.types.Component;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 26/01/2018
 * copyright Dahu Ltd 2018
 * <p>
 *
 *     Simple Vector Processor. Look for a path to a temp file location. If present, try and open a file.
 *     If we can find a file where we are told to look, then read the bytes and push them to the iDocument
 *     Assumption is that some downstream process will deal with the raw bytes, either in Vector or in the indexers we push to
 *     If used, this processor MUST be called BEFORE any call to a Tika processor
 *
 * Changed by :
 */

public class FileInjector extends AbstractProcessor {

    private static final int MAX_FILE_SIZE = 50000000;  // Do not parse files greater than 50MB


    // If this FileInjector is going to handle CIFS/SMB files, we need some auth info from the config
    private String cifs_username = null;
    private String cifs_password = null;
    private String cifs_domain = null;
    private boolean isCifsReady = false;
    private int max_file_size = MAX_FILE_SIZE;
    NtlmPasswordAuthentication auth = null;

    public FileInjector(Level _level, Component _component) throws BadConfigurationException {
        super(_level, _component);
    }


    @Override
    public boolean initialiseMe() throws BadConfigurationException {

        if (getIsPropertiesSet()) { // has the Plugin given us our config?
            for (String k : getProperties().keySet()) {
                if (k.equals("cifs_domain")) {
                    cifs_domain = getProperties().get(k);
                } else if (k.equals("cifs_username")) {
                    cifs_username = getProperties().get(k);
                } else if (k.equals("cifs_password")) {
                    cifs_password = getProperties().get(k);
                } else if (k.equals("max_file_size")) {
                    try {
                        max_file_size = Integer.parseInt(getProperties().get(k));
                    } catch (NumberFormatException nfe) {
                        max_file_size = MAX_FILE_SIZE;
                    }
                }
            }
            if (cifs_domain != null && cifs_username != null && cifs_password != null) {
                auth = new NtlmPasswordAuthentication(cifs_domain, cifs_username, cifs_password);
                if (auth != null) {
                    isCifsReady = true;
                }
            }
            return true;
        }
        return false;
    }



    /**
     * a Processing stage that will read a file from a FS reference and load the data into an iDocument object
     * This Processor can load a document from a CIFS share, or from a local file-system
     *
     * To load a CIFS document, the inbound iDocument object should contain the following metadata elements
     *
     * def:connector_type => "CIFS"
     * def:cifs_username => username in an AD domain
     * def:cifs_password => password in clear text
     * def:cifs_domain => an AD domain name
     * def:def_path => a UNC path to a file/resource in a CIFS share
     *
     * If we can open the file idenfified by def:def_path, its body is loaded into the iDocument data field
     *
     * To load a local Filesystem document, the inbound iDocument object should contain the following metadata elements
     *
     * tmpFilePath ie we call doc.getTmpFilePath() to get a relative or absolute path to a FS resource (file)
     *
     * If we can open the file identified by tmpFilePath, its body is loaded into the iDocument data field
     *
     * If we fail to load the file for any reason, the iDocument is returned untouched.
     *
     * @param _iDoc an iDocument object containing a FS or CIFS reference as a path
     * @return an iDocument object with the data from the FS/CIFS file loaded into the document Data field
     */
    @Override
    public iDocument process(iDocument _iDoc) {

        logger.trace("FileInjector processing doc, " + _iDoc.getId());
        logger.info(String.format("Processor '%s' Processing '%s'",this.getName(),_iDoc.getId()));

        String filePath = _iDoc.getTmpFilePath();

        if (filePath == null) {
            logger.trace("FileInjector : no tmp file path for document, " + _iDoc.getId());
            return _iDoc;
        }

        // Is it an SMB file at the tmp file location?
        if (filePath.startsWith("smb://") && isCifsReady){
            String targetCifsDomain = (String)_iDoc.getFields().get("cifs_domain").toArray()[0];
            if (targetCifsDomain.equals(cifs_domain)) {  // we should have CIFS credentials that might work for this document

                SmbFile smbFile = null;
                try {
                    smbFile = new SmbFile(filePath, auth);
                } catch (MalformedURLException murle) {
                    logger.warn("FileInjector failed to open " + filePath + " : it could be an invalid SMB file path : " + murle.getLocalizedMessage());
                    return _iDoc;
                }
                try {
                    if (smbFile != null && smbFile.canRead()) {

                        if (smbFile.getContentLength() > max_file_size) {
                            logger.debug("Skipping file injection because file is too large. ");
                            return _iDoc;
                        }

                        InputStream is = smbFile.getInputStream();
                        byte[] data = IOUtils.toByteArray(is);
                        _iDoc.setData(data);
                        is.close();
                        logger.debug("FileInjector injected " + data.length + " bytes from " + filePath + " into doc id, " + _iDoc.getId());
                        return _iDoc;
                    } else {
                        logger.warn("Failed to open an SMBFile - SMbFile canRead returns false for " + filePath);
                    }
                } catch (SmbException smbe) {
                    logger.warn("File Injector error opening SMB file, " + filePath + " : " + smbe.getLocalizedMessage());
                } catch (IOException ioe) {
                    logger.warn("File Injector IO error on SMB file, " + filePath + ": " + ioe.getLocalizedMessage());
                }
            }
        } else {

            try {
                Path path = Paths.get(filePath);
                _iDoc.setData(Files.readAllBytes(path));
                logger.debug("FileInjector injected " + _iDoc.getDataSize() + " bytes from " + filePath + " into doc id, " + _iDoc.getId());
            } catch (IOException ioe) {
                logger.warn("FileInjector IO error reading file at " + filePath + " :: " + ioe.getLocalizedMessage());
            } catch (OutOfMemoryError oome) {
                logger.warn("File Injector Out of memory reading file at " + filePath);
            } catch (SecurityException se) {
                logger.warn("File Injector Security Exception : cannot read file at " + filePath);
            } catch (Exception e) {
                logger.warn("FileInjector error: " + e.getLocalizedMessage());
            }
        }

        return _iDoc;
    }
}
