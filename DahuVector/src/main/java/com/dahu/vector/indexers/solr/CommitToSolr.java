package com.dahu.vector.indexers.solr;

import com.dahu.core.logging.DEFLogManager;
import com.dahu.def.config.PluginConfig;
import com.dahu.def.exception.BadArgumentException;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.exception.ContextException;
import com.dahu.def.plugins.EventPluginBase;
import com.dahu.def.types.Event;
import com.dahu.def.types.Properties;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 28/03/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 * Event : scheduled to run periodically, connect to SolrCloud and force a Commit on an index
 * The SolrCloudIndexer does not Commit when it sends updates to Solr because it would be inefficient
 * Hence this must be run otherwise SOlr will build up a huge backlog of uncommitted changes
 * Sample Config :
 * {
 *   "DEFSettings":{
 *     "interval":"60000",
 *     "runAtStartup":"false"
 *   },
 *   "pluginSettings":{
 *     "solr_index":"test_fs",
 *     "solr_hostname":"dolly.office.dahu.co.uk",
 *     "solr_port":"8983",
 *     "logLevel":"TRACE"
 *   }
 * }
 *
 */

public class CommitToSolr extends EventPluginBase {

    private static final String CONFIG_HOSTNAME = "solr_hostname";
    private static final String CONFIG_SOLRPORT = "solr_port";
    private static final String CONFIG_INDEXNAME = "solr_index";

    private String solrHostname = null;
    private String solrPort = null;
    private Set<String> indexNames = null;

    private boolean isInitialised = false;
    private ConcurrentUpdateSolrClient solr = null;


    public CommitToSolr(Level _level,Event _plugin)  {
        super(_level, _plugin);

        Properties props = PluginConfig.getPluginProperties(_plugin.getName());
        solrHostname = props.getPropertyByName(CONFIG_HOSTNAME);
        solrPort = props.getPropertyByName(CONFIG_SOLRPORT);
        indexNames = props.getPropertiesByName(CONFIG_INDEXNAME);

        if (indexNames != null && solrPort != null && solrHostname != null){
            isInitialised = true;
        }
    }

    @Override
    public void doStartup(Event event) throws ContextException, BadArgumentException, BadConfigurationException, UnsupportedEncodingException {
        logger.info("Starting CommitToSolr....");
    }

    @Override
    public void doShutdown(Event event) {
            logger.info("Stopping CommitToSolr...");
    }

    @Override
    public void doRun(Event event) {

        logger.debug("Solr Commit starting....");
        SolrCloudIndexer.setIsCommitting(true);
        // wait for solr client timeout so that all threads that are sending to Solr will finish their job and wait before going again
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ie) {

        }

        for (Object indexNameObj : indexNames) {
            if (indexNameObj instanceof String) {
                String indexName = (String)indexNameObj;
                solrConnect(indexName);

                if (solr != null) {
                    try {
                        solr.commit();
                        logger.trace("Solr Commit complete on " + indexName);
                        solr.close();
                        solr = null;
                    } catch (IOException ioe) {
                        logger.warn("Commit failed : IO " + ioe.getLocalizedMessage());
                        //  ioe.printStackTrace();
                    } catch (SolrServerException sse) {
                        logger.warn("Commit failed : Solr error " + sse.getLocalizedMessage());
                        //   sse.printStackTrace();
                    }
                } else {
                    logger.warn("Unable to commit to solr - unable to connect for index, " + indexName);
                }
            }
        }

        logger.debug("Solr Commit complete - releasing the isCommitting lock....");
        SolrCloudIndexer.setIsCommitting(false);

    }

    /**
     * Open connection to Solr through its ZK array (SolrCloud mode)
     */
    private void solrConnect(String _indexName){
        logger.trace("Solr connecting for index, " + _indexName);
        String solrUrl = "http://" + solrHostname + ":" + solrPort + "/solr/" + _indexName;
        solr = new ConcurrentUpdateSolrClient.Builder(solrUrl).withQueueSize(10).build();
        solr.setConnectionTimeout(5000);
        solr.setSoTimeout(10000);

    }

}
