package com.dahu.vector.indexers.pes;

import com.dahu.core.abstractcomponent.AbstractTerminator;
import com.dahu.core.document.DOCUMENT_CONSTANTS;
import com.dahu.core.exception.BadDocumentException;
import com.dahu.core.interfaces.IndexType;
import com.dahu.core.interfaces.iDocument;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.core.utils.LogUtils;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.types.Component;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static com.dahu.vector.indexers.pes.PES_CONSTANTS.*;
import static java.lang.String.format;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 05/09/2018
 * copyright Dahu Ltd 2018
 * <p>
 * Changed by :
 */

public class PESIndexer extends AbstractTerminator {

    private static final IndexType type = IndexType.ISYS;

    private Logger dataLogger = null;

    private String PESServerName = null;
    private String PESApiPassword = null;
    private String PESApiPort = null;

    private String PESIndexName = null;


    private String PESPushUrl;
    private String indexName;
    private boolean isInitialised = false;


    public PESIndexer(Level _level, Component _component){
        super(_level, _component);
    }


    @Override
    public boolean initialiseMe() throws BadConfigurationException{

        // look for a minimum set of config properties needed to talk to PES Web Api
        for (String p: properties.keySet() ){
            if (p.equalsIgnoreCase(CONFIG_PES_SERVERNAME)){
                PESServerName = properties.get(p);
            } else if (p.equalsIgnoreCase(CONFIG_PES_ISYS_WEBAPI_PORT)){
                PESApiPort = properties.get(p);
            } else if (p.equalsIgnoreCase(CONFIG_PES_API_PASSWORD)){
                PESApiPassword = properties.get(p);
            } else if (p.equalsIgnoreCase(CONFIG_PES_INDEXNAME_TO_INDEX)){
                PESIndexName = properties.get(p);
            } else if (p.equalsIgnoreCase("loglevel")){
                DEFLogManager.changeLogLevel(logger,LogUtils.getLogLevel(properties.get(p)));
            }

        }

        if (PESServerName != null && PESApiPassword != null && PESApiPort != null) {

            PESPushUrl = "http://" + PESServerName + ":" + PES_CONSTANTS.PES_WEB_API_PORT_STR + "/api";
            logger.trace("Constructed new PESIndexer : Push Url = " + PESPushUrl);
            this.isInitialised = true;
        } else {
            isInitialised = false;
        }

        return isInitialised;

    }


    @Override
    public boolean terminate(iDocument _iDoc) {

        if (this.isInitialised == false) {
            logger.warn("PESIndexer is not initialised");
            return false;
        }

        String status = null;
        if (_iDoc.getAction().equalsIgnoreCase(DOCUMENT_CONSTANTS.ACTION_DELETE)) {
            // put a message in the Push API Data Store for a client to see what happened
            status = deleteDoc(_iDoc);
        } else if (_iDoc.getAction().equalsIgnoreCase(DOCUMENT_CONSTANTS.ACTION_UPDATE)) {
            // update the last mod
            status = insertDoc(_iDoc);
        } else {
            // put a message in the Push API Data Store for a client to see what happened
            status = insertDoc(_iDoc);
        }
        logger.info("PESIndexer : " + _iDoc.getId() + " Status :- " + status);
        if (status.equals("success")){
            return true;
        } else {
            return false;
        }
    }

    private String insertDoc(iDocument _iDoc){

        String docId = _iDoc.getId();
        String finalPESIndexName;

        if (_iDoc.getFields().get("indexname") == null && this.PESIndexName == null){
            return "Error: No PES index specified";
        } else {
            if (_iDoc.getFields().get("indexname") != null){
                finalPESIndexName = (String)_iDoc.getFields().get("indexname").toArray()[0];
            } else {
                finalPESIndexName = this.PESIndexName;
            }
        }

        return indexAddOneDoc(docId,indexName,_iDoc);
    }


    private String deleteDoc(iDocument _iDoc){
        int code = -1;


        String finalPESIndexName;

        if (_iDoc.getFields().get("indexname") == null && this.PESIndexName == null){
            return "Error: No PES index specified";
        } else {
            if (_iDoc.getFields().get("indexname") != null){
                finalPESIndexName = (String)_iDoc.getFields().get("indexname").toArray()[0];
            } else {
                finalPESIndexName = this.PESIndexName;
            }
        }

        HttpURLConnection con = null;

        try {


            URL url = new URL(this.PESPushUrl);
            con = (HttpURLConnection) url.openConnection();
            con.setAllowUserInteraction(false);
            con.setRequestMethod("GET");
            con.addRequestProperty("action", "delete");
            con.addRequestProperty("index", finalPESIndexName);
            con.addRequestProperty("filename", _iDoc.getId());
            if (PESApiPassword != null){
                con.addRequestProperty("password", PESApiPassword);
            }

            code = con.getResponseCode();
            if (code == 200){
                if (dataLogger != null){
                    dataLogger.info("deleted " + _iDoc.getId() + " from " + finalPESIndexName);
                }
                logger.debug("DELETE " + _iDoc.getId() + " from " + PESServerName + "->" + finalPESIndexName);
                return "success";

            } else {
                logger.info("Error while deleting from PES. Doc ID " + _iDoc.getId() + "; Index " + finalPESIndexName + " : PES " + PESServerName);
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                    String inputLine;
                    while ((inputLine = in.readLine()) != null){
                        logger.debug("PES Response :- " + inputLine);
                    }
                    in.close();
                }catch (IOException ioe){
                    logger.warn("Error deleting from PES - no response from PES");
                }
                return "PESIndexed error on delete : code = " + code;
            }

        } catch (Exception e){
            logger.warn(format("problem when posting to perceptive [%s]",e.getMessage()));
            logger.warn(e);
            e.printStackTrace();
            //return false;
        }   finally {
            if (null != con){
                con.disconnect();
            }
        }

        return "PESIndexed error on delete";
    }



    private String indexAddOneDoc(@NotNull String _docId, @NotNull String _indexName, iDocument _iDoc){

        String filename = _docId;
        String messageIndexName = _indexName; // If the incoming message defines a target index, use it, otherwise use the one given in config


        int code = -1;
        HttpURLConnection con = null;


        URL url;
        try {
            url = new URL(this.PESPushUrl);
            con = (HttpURLConnection) url.openConnection();
            con.setAllowUserInteraction(false);
        } catch (MalformedURLException murle){
            logger.warn("Bad URL for PES " + this.PESPushUrl + " :: " + murle.getLocalizedMessage());
        } catch (IOException ioe){
            logger.warn("IO for PES " + this.PESPushUrl + " :: " + ioe.getLocalizedMessage());
        }


        con.addRequestProperty("action", "add");
        con.addRequestProperty("index", messageIndexName);
        logger.debug("Sending to PES index " + messageIndexName);
        if (dataLogger != null) {
            dataLogger.info("Sending doc, " + filename + " to PES : " + PESServerName + "/" + messageIndexName);
        }

        // set a unique key
        con.addRequestProperty("filename", filename);
        logger.trace("PES filename is " + filename);
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);

        // SHOULD THIS BE A LAST-MODIFIED DATE FROM THE DOC? We are sending the Index date instead
        con.addRequestProperty("last-modified", df.format(new Date()));
        con.addRequestProperty("password", this.PESApiPassword);


        // add some  metadata
        // in PES PUSH API, all metadata values are pushed as metas in the HTTP request HEAD
        // there are a small number of required named metas (eg filename, last-modified, size etc)
        // all custom ones are pushed as numbered metas
        // meta1 : metaname=metavalue
        // meta2 : metaname=metavalue
        // meta3 etc  - PES will split these and try to add to a pre-defined meta with a name corresponding to  "metaname"
        // PES requires these numbered metas to be in order with no gaps
        int count = 1;

        for (String name : _iDoc.getFieldNames()){
            for (String mv : _iDoc.getFields().get(name)){
                String PESMetaValue = name + "=" + mv;
                String PESMetaName = "meta" + String.valueOf(count++);
                PESMetaValue = PESMetaValue.replace("\\n","").replace("\\r","");
                con.addRequestProperty(PESMetaName,PESMetaValue);
                logger.trace(format("PES : Add META %s : %s",PESMetaName,PESMetaValue));
            }
        }

        con.setDoInput(true);
        con.setDoOutput(true);

        // data length
        int length = 0;
        long startTime = System.nanoTime();

        boolean docHasData = false;
        try {
            docHasData = _iDoc.hasData();
        } catch (BadDocumentException bde){
            // do nothing - there is no data
        }

        if ( _iDoc.isStub() || ! docHasData || _iDoc.getDataSize() > 102400000) {
            // There is no document body, only metadata OR file is too large to index - >100MB
            logger.debug("DATA IS NULL OR DOC TOO LARGE OR DOC IS STUB - setting indexedContent=false");
            String keyName = "meta" + String.valueOf(count);
            String value = "indexedContent=false";
            con.addRequestProperty(keyName, value);
            //set the size
            String payload = format("filename:%s\n", filename);
            length = payload.length();
            con.setRequestProperty("Content-Length", "" + length);

            DataOutputStream wr;
            try {
                wr = new DataOutputStream(con.getOutputStream());
                wr.writeBytes(payload);
                wr.flush();
                wr.close();
            } catch (IOException ioe) {
                logger.warn("IO writing to PES " + ioe.getLocalizedMessage());
            }

        } else {
            String keyName = "meta" + String.valueOf(count);
            String value = "indexedContent=true";
            con.addRequestProperty(keyName,value);
            try {
                DataOutputStream wr = new DataOutputStream(con.getOutputStream());
                wr.write(_iDoc.getData(), 0, length);
                wr.flush();
                wr.close();
            } catch (IOException ioe){
                logger.warn("IO writing to PES  " + ioe.getLocalizedMessage());
                return "PES Indexer IO error : " + ioe.getLocalizedMessage();
            } catch (BadDocumentException bde){
                logger.warn("BadDocument Error reading data from DEFDocument to write to PES  " + bde.getLocalizedMessage());
                return "BadDocument Error reading data from DEFDocument to write to PES : " + bde.getLocalizedMessage();
            }
        }
        try {
            code = con.getResponseCode();
        } catch (IOException ioe){
            logger.warn("IO writing to PES  " + ioe.getLocalizedMessage());
        }
        long endTime = System.nanoTime();

        if (code != 200) {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String line;
                while ((line = br.readLine()) != null) {
                    logger.warn(format("[instance %d]perceptiveIndexer failed (not 200) returned %s", code, line));
                }
                br.close();
            } catch (IOException ioe) {
                logger.warn("Failed to read response content from PES when response code was not 200 : " + ioe.getLocalizedMessage());
            }
            return "PES Indexer error code = " + code;
        } else {
            long duration = (endTime - startTime)/1000000;  //divide by 1000000 to get milliseconds.
            logger.debug(format(" indexed document : [%d] bytes in %d milliseconds %s ",  length, duration, filename));
        }
        return "success";

    }

}
