package com.dahu.plugins.edge.aodocs.storage;

import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.exception.BadArgumentException;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.exception.ContextException;
import com.dahu.def.plugins.StorePluginBase;
import com.dahu.def.types.Store;
import org.apache.logging.log4j.Level;

import java.io.UnsupportedEncodingException;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 18/11/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class AODocsConnectorStore extends StorePluginBase<CSVDiskStore> {

    private String CONFIG_STORE_PATH = "aodocsStoragePath";
    private String AODocsStoreFilePath;
    private String name;
    private static CSVDiskStore thisStore = null;

    public AODocsConnectorStore(Level _level, Store _plugin){
        super(_level, _plugin);
        logger = DEFLogManager.getLogger("AODocsStorage-"+_plugin.getName(),_level);

        AODocsStoreFilePath = this.getProperty(CONFIG_STORE_PATH);
        this.name = _plugin.getName();
        logger.debug("Initializing AODocsConnector Storage " + this.name);
        try {
            thisStore = new CSVDiskStore(AODocsStoreFilePath, logger);
        } catch (BadConfigurationException bce){
            logger.warn("Unable to create new CSV Disk Store for AODocs Connector Storage");
            DEFLogManager.LogStackTrace(logger, "AODocsConnectorStorage",bce);
        }
    }


    @Override
    public void doStartup(Store store) throws ContextException, BadArgumentException, BadConfigurationException, UnsupportedEncodingException {

        logger.trace("Starting AODocsConnector Storage : " + this.name);
        thisStore.reload();
    }

    @Override
    public void doShutdown(Store store) {
        logger.trace("Starting AODocsConnector Storage : " + this.name);
        thisStore.save();

    }

    @Override
    public void doRefresh(Store store) {
        thisStore.reload();
    }

    @Override
    public CSVDiskStore getStore() {
        return this.thisStore;
    }
}
