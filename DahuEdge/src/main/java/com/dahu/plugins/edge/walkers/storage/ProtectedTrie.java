package com.dahu.plugins.edge.walkers.storage;

import com.dahu.core.trie.Trie;
import org.apache.logging.log4j.Logger;

import java.util.Set;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 13/08/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class ProtectedTrie {

    private Object myLock = new Object();
    private Trie crate = null; // wrapper around a single Trie structure
    private boolean isStale = false; // has the crate been changed but not saved?

    Logger logger = null;

    public ProtectedTrie(String _triePath, Logger _logger){
        logger = _logger;
        if (crate == null){
            logger.trace("ProtectedTrie is instantiating a new Trie at " + _triePath);
            crate = new Trie(_triePath,_logger);
        }
    }

    public final Trie getReadOnlyCrate(){return crate;}

    protected void saveTrie(){
        logger.trace("calling saveTrie on crate, " + crate.getName());
        if (isStale){
            synchronized (myLock) {
                logger.trace("saving trie on crate, " + crate.getName());
                crate.saveMe(true);
                isStale = false;
            }
        }
    }

    public void insertBranches(Set<String> _branches){
        synchronized (crate){
            for (String s : _branches) {
                crate.insertBranch(s);
            }
            isStale = true;
        }
    }

    public void insertLeaves(Set<String> _leaves){
        synchronized (crate){
            for (String s : _leaves){
                String leaf = new String(s);
                if (leaf.indexOf("\\")>0) {
                    leaf = leaf.replaceAll("\\\\","/");
                }
                String path = null;
                String lastModStr = null;
                String sizeStr = null;
                String name = null;
                long lastMod = 0;
                long size = 0;
                if (leaf.indexOf("_:_")>0){
                    path = leaf.substring(0,leaf.indexOf("_:_"));
                    leaf = leaf.substring(leaf.indexOf("_:_")+3);
                    if (leaf.indexOf("_:_")>0){
                        lastModStr = leaf.substring(0,leaf.indexOf("_:_"));
                        sizeStr = leaf.substring(leaf.indexOf("_:_")+3);
                    }
                }
                if (null != path && path.indexOf("/")>0){
                    name = path.substring(path.lastIndexOf("/")+1);
                    path = path.substring(0,path.lastIndexOf("/"));
                }

                if (null != lastModStr){
                    try {
                        lastMod = Long.parseLong(lastModStr);
                    } catch (NumberFormatException nfe){
                        // do nothing. LastMod = 0 is not ideal for a TrieLeaf but if we don't have a lst mod date, we can still insert
                    }
                }
                if (null != sizeStr){
                    try {
                        size = Long.parseLong(sizeStr);
                    } catch (NumberFormatException nfe){
                        // do nothing - size of zero is not a problem for a Trie Leaf
                    }
                }
                if (null != path && null != name){
                    crate.insertLeaf(path, name, lastMod, size);
                }
            }
            isStale = true;
        }
    }

    public void deleteNodes(Set<String> _nodePaths){
        synchronized (crate){
            for (String s : _nodePaths){
                crate.removeNode(s);
            }
            isStale = true;
        }
    }
}
