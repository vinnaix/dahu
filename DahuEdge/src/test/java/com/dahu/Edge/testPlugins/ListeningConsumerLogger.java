package com.dahu.Edge.testPlugins;

import com.dahu.core.document.DEFDocument;
import com.dahu.core.document.DEFFileDocument;
import com.dahu.core.exception.BadDocumentException;
import com.dahu.core.exception.MissingFileException;
import com.dahu.core.interfaces.iDocument;
import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.config.PluginConfig;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.exception.MQException;
import com.dahu.def.plugins.ListeningService;
import com.dahu.def.types.Service;
import org.apache.logging.log4j.Level;

import javax.jms.JMSException;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 03/05/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class ListeningConsumerLogger extends ListeningService {


    private final String CONFIG_OUTPUTFILEPATH = "filepath";
    private String outputFilePath;

    public ListeningConsumerLogger(Level _level, Service _service, int _threadNum) throws JAXBException, MQException, BadConfigurationException, JMSException {
        super(_level, _service, _threadNum);

        outputFilePath = PluginConfig.getPluginProperties(serviceName).getPropertyByName(CONFIG_OUTPUTFILEPATH);
        logger.debug("Starting ListeningConsumerLogger");

        if (null == outputFilePath) {
            outputFilePath = "./output.txt";
        }
        logger.debug("ListeningConsumer writes to output file at " + outputFilePath);
        File f = new File(outputFilePath);
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
        } catch (IOException ioe){
            ioe.printStackTrace();
        }
    }


    @Override
    public void processMessage(String _message){
        logger.debug("ListeningConsumerLogger - retrieved message from queue : " + _message);
        try {
            DEFDocument newDoc = new DEFFileDocument(_message);
            processMessage(newDoc);
        } catch (BadDocumentException bde){
            logger.warn("Failed to create DEFDoc from message : " + _message);
        } catch (MissingFileException mfe){
            logger.warn("File in Json message actually doesn't exit " + mfe.getLocalizedMessage() + " :: json => " + _message);
        }


    }

    @Override
    public void processMessage(iDocument _iDoc){

        logger.debug("ListeningConsumerLogger retrieved iDoc from queue");
        String docId = _iDoc.getId();
        try {
            if (null != docId) {
                docId = docId + "\n";
                Files.write(Paths.get(outputFilePath), docId.getBytes(), StandardOpenOption.APPEND);
            }
        }catch (IOException ioe){
            logger.warn("IO Exception writing to " + outputFilePath);
            DEFLogManager.LogStackTrace(logger, "CrawlTest",ioe);
        }
    }

}
