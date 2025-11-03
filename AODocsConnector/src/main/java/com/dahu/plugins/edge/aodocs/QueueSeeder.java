package com.dahu.plugins.edge.aodocs;

import com.dahu.aodocs.types.AODLibrary;
import com.dahu.def.config.PluginConfig;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.plugins.YieldingService;
import com.dahu.def.types.Queue2_0;
import com.dahu.def.types.Service;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.logging.log4j.Level;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 06/12/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class QueueSeeder extends YieldingService {

    public QueueSeeder(Level _level, Service _service, int _threadNum) throws BadConfigurationException, Exception {
        super(_level, _service, _threadNum);

        if (inputQueues.size() != 1){
            throw new BadConfigurationException("QueueSeeder needs to have ONE inputQueue defined");
        }
    }


    @Override
    protected int doWorkThenYield() throws BadConfigurationException {
        Queue2_0 seedQueue = this.getFirstInputQueue();
        logger.debug("QueueSeeder is seeding queue with libraries");
        JsonNode librariesArray = PluginConfig.getPluginProperties(serviceName).getPropertiesAsJson("libraries");
        if (null != librariesArray && librariesArray.isArray()){
            for (int i = 0; i < ((ArrayNode)librariesArray).size(); i++){
                JsonNode libraryNode = librariesArray.get(i);
                AODLibrary library = new AODLibrary(libraryNode);
                seedQueue.postTextMessage(library.getJson().toString());
                logger.trace("Pushed library, " + library.getLibraryName() + " to internal AODocsConnector queue");
            }
        }

        return Service.SHUT_ME_DOWN;
    }
}
