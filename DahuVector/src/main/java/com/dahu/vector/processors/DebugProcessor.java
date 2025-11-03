package com.dahu.vector.processors;


import com.dahu.core.abstractcomponent.AbstractProcessor;
import com.dahu.core.exception.BadMetaException;
import com.dahu.core.interfaces.iDocument;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.types.Component;
import org.apache.logging.log4j.Level;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 04/04/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class DebugProcessor extends AbstractProcessor {


    public DebugProcessor(Level _level, Component _component) throws BadConfigurationException {
        super(_level, _component);
    }


    @Override
    public iDocument process(iDocument _idoc) {

        if (null != logger && logger.isDebugEnabled()) {
            logger.debug("Enter DebugProcessor for " + _idoc.getId());

            logger.debug("Dumping iDoc, " + _idoc.getId());
            logger.debug("-- GUID:- " + _idoc.getGuid());
            logger.debug("-- Title:- " + _idoc.getTitle());
            logger.debug("-- AltTitle:- " + _idoc.getAltTitle());
            logger.debug("-- Action:- " + _idoc.getAction());
            logger.debug("-- Url:- " + _idoc.getUrl());
            logger.debug("-- Mime:- " + _idoc.getMimeType());
            logger.debug("-- LastMod String:- " + _idoc.getLastModifiedZulu());
            logger.debug("-- LastMod Long:- " + _idoc.getLastModified());
            logger.debug("-- Size:- " + _idoc.getDataSize());
            logger.debug("-- Source:- " + _idoc.getSource());
            for (String s : _idoc.getFieldNames()) {
                try {
                    for (String v : _idoc.getFieldValues(s)) {
                        logger.debug("-- --" + s + ":=> " + v);
                    }
                } catch (BadMetaException bme) {
                    logger.debug("Unable to read field values for " + s);
                }
            }
            /*
            if (null != _idoc.getBody()){
                if (_idoc.getBody().length() > 2048){
                    logger.debug("-- Body:- " + _idoc.getBody().substring(0,2048));
                } else {
                    logger.debug("-- Body:- " + _idoc.getBody());
                }
            }

             */
        }
        return _idoc;
    }

    @Override
    public boolean initialiseMe() throws BadConfigurationException {
        return true;
    }
}
