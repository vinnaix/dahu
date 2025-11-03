package com.dahu.plugins.edge.walkers.storage;

import com.dahu.core.logging.DEFLogManager;
import com.dahu.core.trie.Trie;
import com.dahu.def.config.PluginConfig;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.plugins.StorePluginBase;
import com.dahu.def.types.Store;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FSWalkerStorage extends StorePluginBase<ProtectedTrie> {

    private Logger logger;

    private final String CONFIG_CRATEDIR = "crate_path";
    private final String CONFIG_TRIEPATH = "trie_file";

    private String name;

    private String cratedir;
    private String triepath;

    private ProtectedTrie pt = null;

    ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(4);

    protected boolean isScheduledServiceRunning = false;


    public FSWalkerStorage(Level _level, Store _plugin) {
        super(_level, _plugin);
        this.name = _plugin.getName();
        logger = DEFLogManager.getLogger("Storage-FSWalker-"+name,_level);
        logger.trace(Thread.currentThread().getId() + " :: Enter FSWalkerStorage constructor..... for " + name);

        cratedir = this.getProperty(CONFIG_CRATEDIR);
        triepath = this.getProperty(CONFIG_TRIEPATH);

        logger.debug(Thread.currentThread().getId() + " :: Creating new Trie-based WalkerStorage at " + cratedir + "/" + triepath);

        // make sure directory exists
        File dataDirectory = new File(cratedir);
        if (! dataDirectory.exists()){
            logger.trace(Thread.currentThread().getId() + " :: WalkerStorage Trie folder does not exist - creating new folder at " + cratedir);
            dataDirectory.mkdir();
        }

        pt = new ProtectedTrie(cratedir + "/" + triepath, logger);

        if (scheduledExecutorService.isShutdown()){
            scheduledExecutorService = Executors.newScheduledThreadPool(1);
        }

        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                logger.trace(Thread.currentThread().getId() + " :: FSWalker Storage Background Thread is saving the Trie - " + pt.getReadOnlyCrate().getName() + " in Storage, " + name);
                pt.saveTrie();
                logger.trace(Thread.currentThread().getId() + " :: FSWalker Storage Background Thread finished saving the Trie - " + pt.getReadOnlyCrate().getName() + " in Storage, " + name);

            }
        }, 120,120, TimeUnit.SECONDS);

    }

    @Override
    public void doStartup(Store store) throws BadConfigurationException {


        logger.debug(Thread.currentThread().getId() + " :: FSWalkerStorage doStartup...." + name);
        // do we need to start background thread for saving the crate??


    }

    @Override
    public void doShutdown(Store _store) {
        logger.trace(Thread.currentThread().getId() + " :: doShutdown called - saving the Trie called " + pt.getReadOnlyCrate().getName() + " in Storage, " + name);
        pt.saveTrie();

        if (isScheduledServiceRunning) {

            // Scheduled tasks for rebuilding priority buckets - also needs to be stopped
            try {
                scheduledExecutorService.shutdown();
                scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {

            } finally {
                scheduledExecutorService.shutdownNow();
            }
        }
        isScheduledServiceRunning = false;

        logger.info("FSWalkerStorage shutdown for " + name);

    }

    @Override
    public void doRefresh(Store store) {
        logger.debug("FSWalkerStorage refreshing crates...");
        // TODO what does this mean?
    }


    @Override
    public ProtectedTrie getStore (){
        return this.pt;
    }

}
