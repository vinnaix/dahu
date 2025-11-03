package com.dahu.plugins.edge.aodocs;

import com.dahu.aodocs.types.AODLibrary;
import com.dahu.def.config.PluginConfig;
import com.dahu.def.config.ServerConfig;
import com.dahu.def.exception.BadArgumentException;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.exception.ContextException;
import com.dahu.def.plugins.EventPluginBase;
import com.dahu.def.types.Event;
import com.dahu.def.types.Properties;
import com.dahu.def.types.Queue2_0;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.logging.log4j.Level;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.io.UnsupportedEncodingException;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 10/01/2020
 * copyright Dahu Ltd 2020
 * <p>
 * Changed by :
 */

public class QueueSeederEvent extends EventPluginBase {

    Queue2_0 seedQueue = null;
    String eventName = null;

    public QueueSeederEvent(Level _level, Event _plugin){
        super(_level, _plugin);
        eventName = _plugin.getName();
        seedQueue = (Queue2_0) ServerConfig.getQueues().get(PluginConfig.getFirstInputQueue(_plugin.getName()));
    }


    @Override
    public void doStartup(Event event) throws ContextException, BadArgumentException, BadConfigurationException, UnsupportedEncodingException {

    }

    @Override
    public void doShutdown(Event event) {

    }

    @Override
    public void doRun(Event event) {

        if ( seedQueue.getQueueCount() <= 1) {
            JsonNode librariesArray = PluginConfig.getPluginProperties(this.eventName).getPropertiesAsJson("libraries");
            if (null != librariesArray && librariesArray.isArray()) {
                for (int i = 0; i < ((ArrayNode) librariesArray).size(); i++) {
                    JsonNode libraryNode = librariesArray.get(i);
                    AODLibrary library = new AODLibrary(libraryNode);
                    seedQueue.postTextMessage(library.getJson().toString());
                    logger.debug("Refreshing library, " + library.getLibraryName());
                }
            }
        } else {
            logger.debug("Not refreshing libraries this time because the previous tasks have not yet completed");
            logger.debug("Queue size is " + seedQueue.getQueueCount());
        }

    }
}
