package com.dahu.vector.testProcessors;

import com.dahu.core.interfaces.iDocument;
import com.dahu.core.abstractcomponent.AbstractProcessor;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.types.Component;
import org.apache.logging.log4j.Level;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 05/07/2018
 * copyright Dahu Ltd 2018
 * <p>
 * Changed by :
 *
 * Simple class to demonstrate reading a value from config properties file and
 * adding the name-value for the property as a meta on a document
 *
 */

public class DummyProcessor extends AbstractProcessor {

    public DummyProcessor(Level _level, Component _component) throws BadConfigurationException {
        super(_level, _component);
    }

    // Look in the properties file for a config item named this
    private static final String CONFIGNAME_PROPERTY1 = "property1";

    // variable to store the value for the property in config
    String property1 = null;



    @Override
    public boolean initialiseMe() throws BadConfigurationException {

        if (getIsPropertiesSet()){
            property1 = getProperties().get(CONFIGNAME_PROPERTY1);
            if (property1 != null){
                return true; // we found the property from config so we are good to go
            }
        }
        return false;
    }


    @Override
    public iDocument process(iDocument idoc) {
        idoc.addField(CONFIGNAME_PROPERTY1,property1);
        return idoc;
    }


}
