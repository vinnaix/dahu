package com.dahu.vector.processors;

import com.dahu.core.abstractcomponent.AbstractProcessor;
import com.dahu.core.interfaces.iDocument;
import com.dahu.def.config.ServerConfig;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.types.MQueue;
import com.dahu.def.types.Component;
import org.apache.logging.log4j.Level;
import org.apache.poi.hsmf.MAPIMessage;
import org.apache.poi.hsmf.datatypes.*;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.poifs.filesystem.EntryUtils;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import java.io.*;
import java.util.Date;
import java.util.Map;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 07/09/2018
 * copyright Dahu Ltd 2018
 * <p>
 * Changed by :
 */

public class POIEmailAttachmentExpander extends AbstractProcessor {

    // PES uses this when it exanded ZIP files so we should too
    public static final String EXPANDED_FILE_PARENT_CHILD_SEPARATOR = " -_ ";

    private static final String CONFIG_TEMP_FOLDER = "temp_folder";
    private static final String CONFIG_OUTPUT_QUEUE_NAME = "outputQueue";
    private static final String CONFIG_INSTANCENAME = "instancename";

    private String TEMP_FOLDER = null;
    private String OUTPUT_QUEUE_NAME = null;
    private String INSTANCENAME = null;
    MQueue outputQueue = null;


    public POIEmailAttachmentExpander(Level _level, Component _component) {
        super(_level, _component);
    }

    @Override
    public boolean initialiseMe() throws BadConfigurationException {

        if (getIsPropertiesSet()) { // has the Plugin given us our config?
            for (String k : getProperties().keySet()) {
                if (k.equalsIgnoreCase(CONFIG_TEMP_FOLDER)){
                    TEMP_FOLDER = getProperties().get(k);
                } else if (k.equalsIgnoreCase(CONFIG_OUTPUT_QUEUE_NAME)){
                    OUTPUT_QUEUE_NAME = getProperties().get(k);
                } else if (k.equalsIgnoreCase(CONFIG_INSTANCENAME)){
                    INSTANCENAME = getProperties().get(k);
                }
            }
        }

        if (TEMP_FOLDER == null){
            throw new BadConfigurationException("EmailattachmentExpander requires a temp_folder parameter to be provided in its config");
        }

        File tempFolder = new File("./"+TEMP_FOLDER);
        if (! tempFolder.exists()){
            tempFolder.mkdir();
        } else if (! tempFolder.isDirectory()){
            throw new BadConfigurationException("EmailattachmentExpander temp_folder is not a directory");
        }

        if (OUTPUT_QUEUE_NAME == null){
            throw new BadConfigurationException("EmailattachmentExpander requireds an outputqueue name to be provided in its config");
        }
        outputQueue = ServerConfig.getQueues().get(OUTPUT_QUEUE_NAME);
        if (outputQueue == null){
            throw new BadConfigurationException("EmailattachmentExpander outputQueue does not exist. Name of queue is " + OUTPUT_QUEUE_NAME);
        }

        return true;
    }


    @Override
    public iDocument process(iDocument _idoc) {

        // If there's no data in this iDoc we can't expand it
        if (_idoc.getDataSize() == 0){
            logger.trace("EmailExpander - processed an idoc, " + _idoc.getId() + " but no data, so no possibility to expand");
            return _idoc;
        }

        String baseFileName = _idoc.getId();
        int attachmentCounter = 0;


        // No simple way to sniff a file is MSG file type so try to create a new MAPIMessage and if it fails, its not an email
        MAPIMessage msg = null;
        try {
            msg = new MAPIMessage(new ByteArrayInputStream(_idoc.getData()));
        } catch (Exception e){
            logger.trace("EmailExpander - processed an idoc, "+ _idoc.getId() + " - not non-email file");
            return _idoc;
        }


        // OK, this is an email. Lets look for attachments

        AttachmentChunks[] attachments = msg.getAttachmentFiles();
        if (attachments.length > 0) {
            for (AttachmentChunks ac : attachments) {

                // We don't want to do anything with image files - no data in them
                if (ac.getAttachMimeTag() != null && ac.getAttachMimeTag().getValue().toLowerCase().startsWith("image")) {
                    logger.trace("ignoring image file attachment");
                } else {
                    attachmentCounter++;
                    String childFilename = null;
                    if (ac.getAttachLongFileName() != null) {
                        childFilename = baseFileName + EXPANDED_FILE_PARENT_CHILD_SEPARATOR + cleanFileName(ac.getAttachLongFileName().getValue());
                    } else if (ac.getAttachFileName() != null) {
                        childFilename = baseFileName + EXPANDED_FILE_PARENT_CHILD_SEPARATOR + cleanFileName(ac.getAttachFileName().getValue());
                    } else {
                        childFilename = baseFileName + EXPANDED_FILE_PARENT_CHILD_SEPARATOR + " NO FILENAME [" + attachmentCounter + "]";
                    }

                    // need to write the raw attachment file data to a temp file
                    long timeNow = new Date().getTime();
//                    File tmpFile = new File(TEMP_FOLDER + "/" + timeNow + "_" +attachmentCounter + ".tmp");

                    iDocument childDocument = _idoc.addChild(childFilename); // keep all the existing metadata but create new iDoc

                    if (ac.getAttachmentDirectory() != null) {
                        // the attachment is itself an email file
                        // write it to a temp file
                        // need to convert from MSG to POIFSFileSystem cos MAPIMessage has no method to write out its data

                        FileOutputStream fos = null;
                        try {
                            POIFSFileSystem extractedMsg = rebuildEmailFromAttachment(ac.getEmbeddedMessage());
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            extractedMsg.writeFilesystem(baos);
                            childDocument.setData(baos.toByteArray());
                        } catch(IOException ioe) {
                            // Handle exception here
                            ioe.printStackTrace();
                        } finally {
                            try {
                                fos.close();
                            } catch (IOException ioe){
                                // do nothing
                            }
                        }

                    } else if (ac.getAttachData() != null) {
                        // Its not an email, just a regular file, and it has some data
                        // write the raw file data to temp file
                        childDocument.setData(ac.getAttachData().getValue());
                    }

                }
            }
        }

        return _idoc;
    }

    /**
     * Taken from Msg2txt : create a fully-populated POIFSFileSystem object from a simple MAPIMessage generated from an attachment to an email
     * @param attachedMsg MAPIMessage, an EMAIL MSG object that was found as an AttachmentChunk in a parent EMAIL MSG
     * @return POIFSFileSystem object - a fully-expanded representation of an MS Outlook MSG file
     * @throws IOException
     */
    private static POIFSFileSystem rebuildEmailFromAttachment(MAPIMessage attachedMsg) throws IOException {
        // Create new MSG and copy properties.
        POIFSFileSystem newDoc = new POIFSFileSystem();
        MessagePropertiesChunk topLevelChunk = new MessagePropertiesChunk(null);
        // Copy attachments and recipients.
        int recipientscount = 0;
        int attachmentscount = 0;
        for (Entry entry : attachedMsg.getDirectory()) {
            if (entry.getName().startsWith(RecipientChunks.PREFIX)) {
                recipientscount++;
                DirectoryEntry newDir = newDoc.createDirectory(entry.getName());
                for (Entry e : ((DirectoryEntry) entry)) {
                    EntryUtils.copyNodeRecursively(e, newDir);
                }
            } else if (entry.getName().startsWith(AttachmentChunks.PREFIX)) {
                attachmentscount++;
                DirectoryEntry newDir = newDoc.createDirectory(entry.getName());
                for (Entry e : ((DirectoryEntry) entry)) {
                    EntryUtils.copyNodeRecursively(e, newDir);
                }
            }
        }
        // Copy properties from properties stream.
        MessagePropertiesChunk mpc = attachedMsg.getMainChunks().getMessageProperties();
        for (Map.Entry<MAPIProperty, PropertyValue> p : mpc.getRawProperties().entrySet()) {
            PropertyValue val = p.getValue();

            if (!(val instanceof ChunkBasedPropertyValue)) {
                Types.MAPIType type = val.getActualType();
                if (type != null && type != Types.UNKNOWN) {
                    topLevelChunk.setProperty(val);

                }
            }
        }
        // Create nameid entries.
        DirectoryEntry nameid = newDoc.getRoot().createDirectory(NameIdChunks.NAME);
        // GUID stream
        nameid.createDocument(PropertiesChunk.DEFAULT_NAME_PREFIX + "00020102", new ByteArrayInputStream(new byte[0]));
        // Entry stream
        nameid.createDocument(PropertiesChunk.DEFAULT_NAME_PREFIX + "00030102", new ByteArrayInputStream(new byte[0]));
        // String stream
        nameid.createDocument(PropertiesChunk.DEFAULT_NAME_PREFIX + "00040102", new ByteArrayInputStream(new byte[0]));
        // Base properties.
        // Attachment/Recipient counter.
        topLevelChunk.setAttachmentCount(attachmentscount);
        topLevelChunk.setRecipientCount(recipientscount);
        topLevelChunk.setNextAttachmentId(attachmentscount);
        topLevelChunk.setNextRecipientId(recipientscount);
        // Unicode string format.
        byte[] storeSupportMaskData = new byte[4];
        PropertyValue.LongPropertyValue storeSupportPropertyValue = new PropertyValue.LongPropertyValue(MAPIProperty.STORE_SUPPORT_MASK,
                MessagePropertiesChunk.PROPERTIES_FLAG_READABLE | MessagePropertiesChunk.PROPERTIES_FLAG_WRITEABLE,
                storeSupportMaskData);
        storeSupportPropertyValue.setValue(0x00040000);
        topLevelChunk.setProperty(storeSupportPropertyValue);
        topLevelChunk.setProperty(new PropertyValue(MAPIProperty.HASATTACH,
                MessagePropertiesChunk.PROPERTIES_FLAG_READABLE | MessagePropertiesChunk.PROPERTIES_FLAG_WRITEABLE,
                attachmentscount == 0 ? new byte[] { 0 } : new byte[] { 1 }));
        // Copy properties from MSG file system.
        for (Chunk chunk : attachedMsg.getMainChunks().getChunks()) {
            if (!(chunk instanceof MessagePropertiesChunk)) {
                String entryName = chunk.getEntryName();
                String entryType = entryName.substring(entryName.length() - 4);
                int iType = Integer.parseInt(entryType, 16);
                Types.MAPIType type = Types.getById(iType);
                if (type != null && type != Types.UNKNOWN) {
                    MAPIProperty mprop = MAPIProperty.createCustom(chunk.getChunkId(), type, chunk.getEntryName());
                    ByteArrayOutputStream data = new ByteArrayOutputStream();
                    chunk.writeValue(data);
                    PropertyValue pval = new PropertyValue(mprop, MessagePropertiesChunk.PROPERTIES_FLAG_READABLE
                            | MessagePropertiesChunk.PROPERTIES_FLAG_WRITEABLE, data.toByteArray(), type);
                    topLevelChunk.setProperty(pval);
                }
            }
        }
        topLevelChunk.writeProperties(newDoc.getRoot());
        return newDoc;
    }

    /**
     * Remove invalid characters from a String that we want to use as a filename
     * Windows does not like any of these characters
     * /\:*?"<>|
     * Whilst they will not occur as part of any regular file name that we encounter, we will try to
     * create a new filename from the SUBJECT of an email, which is not restricted.
     * @param _input String that we would like to use as a filename
     * @return String that we can use as a filename
     */
    private static String cleanFileName(String _input){
        return _input.replaceAll("[^a-zA-Z0-9\\.\\-\\'\\_]","_");
    }


}
