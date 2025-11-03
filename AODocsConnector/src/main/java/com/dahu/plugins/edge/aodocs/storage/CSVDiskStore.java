package com.dahu.plugins.edge.aodocs.storage;

import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.exception.BadConfigurationException;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 18/11/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class CSVDiskStore {

    private String filePath = null;
    private Logger logger = null;

    private Map<String,Long> libraryToHWM = new HashMap<>();

    public CSVDiskStore(String _path, Logger _logger) throws BadConfigurationException {

        logger = _logger;
        File f = new File(_path);
        synchronized(this) {
            if (! f.exists()){
                if (! f.getParentFile().exists()){
                    f.getParentFile().mkdirs();
                }
                try {
                    f.createNewFile();
                } catch (IOException ioe) {
                    logger.warn("Unable to create new AODocs Storage file at " + _path);
                    DEFLogManager.LogStackTrace(logger, "AODocsStorage", ioe);
                    throw new BadConfigurationException("\"Unable to create new AODocs Storage file at \" + _path");
                }
            }
        }
        filePath = _path;

    }

    public synchronized void save(String _libraryId, Long _hwm){

        if (libraryToHWM.containsKey(_libraryId) && null != libraryToHWM.get(_libraryId)){
            libraryToHWM.replace(_libraryId,_hwm);
        } else {
            libraryToHWM.put(_libraryId,_hwm);
        }
        save();
    }

    public boolean hasHWM(String _libraryId){
        return this.libraryToHWM.containsKey(_libraryId);
    }

    public Long getHWM(String _libraryId){
        if (this.libraryToHWM.containsKey(_libraryId)) {
            return this.libraryToHWM.get(_libraryId);
        } else {
            this.libraryToHWM.put(_libraryId,0l);
            return 0l;
        }
    }

    public synchronized void reload() {

        libraryToHWM.clear();

        File f = new File(filePath);
        if ( f.exists()) {
            logger.debug("Reloading AODocs Store from " + filePath);
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                String libraryId;
                String hwm;
                while ((line = br.readLine()) != null) {
                    if (line.indexOf(":") > 0) {
                        libraryId = line.substring(0, line.indexOf(":"));
                        hwm = line.substring(line.indexOf(":") + 1);
                        libraryToHWM.put(libraryId, Long.parseLong(hwm));
                    }
                }
            } catch (FileNotFoundException e) {
                if (null != logger) {
                    logger.warn("Unable to read AODocs Storage from " + filePath + " : File missing");
                } else {
                    System.out.println("Unable to read AODocs Storage from " + filePath + " : File missing");
                }
            } catch (IOException e) {
                if (null != logger) {
                    logger.warn("Unable to read AODocs Storage from " + filePath + " : IO error");
                } else {
                    System.out.println("Unable to read AODocs Storage from " + filePath + " : IO error");
                }
            }
        } else {
            logger.debug("Tried reloading AODocs Store from " + filePath + " but no file exists yet - no data to reload");
            try {
                f.createNewFile();
            } catch (IOException ioe){
                logger.warn("Unable to create new AODocs Storage file at " + filePath);
                DEFLogManager.LogStackTrace(logger,"AODocsStorage",ioe);
            }

        }
    }

    public synchronized void save(){

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
            for (String key : libraryToHWM.keySet()) {
                writer.write(key+":"+libraryToHWM.get(key)+"\n");
            }
            writer.close();
        } catch (FileNotFoundException e) {
            if (null != logger) {
                logger.warn("Unable to write to AODocs Storage at " + filePath + " : File missing");
            } else {
                System.out.println("Unable to write to AODocs Storage at " + filePath + " : File missing");
            }
        } catch (IOException e) {
            if (null != logger) {
                logger.warn("Unable to write to AODocs Storage at " + filePath + " : IO error");
            } else {
                System.out.println("Unable to write to AODocs Storage at " + filePath + " : IO error");
            }
        }
    }

}


