package com.dahu.vector.processors;

import com.dahu.core.abstractcomponent.AbstractProcessor;
import com.dahu.core.document.DEFFileDocument;
import com.dahu.core.exception.BadMetaException;
import com.dahu.core.interfaces.iDocument;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.types.Component;
import com.dahu.vector.indexers.solr.DefaultSolrFieldMapper;
import com.dahu.vector.indexers.solr.SOLR_CONSTANTS;
import org.apache.logging.log4j.Level;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 01/07/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class HierarchicalFieldSplitter extends AbstractProcessor {


    private static final String CONFIG_HIER_METANAMES = "hier_metanames";
    private Set<String> metaNames = new HashSet<>();

    public HierarchicalFieldSplitter(Level _level, Component _component) throws BadConfigurationException {
        super(_level, _component);

    }
    @Override
    public iDocument process(iDocument iDocument) {

        String path = null;
        for (String f : metaNames){
            try {
                if (f.equalsIgnoreCase("filepath") && iDocument instanceof DEFFileDocument) {
                    path = ((DEFFileDocument) iDocument).getPath();
                    // For DEFFile, this path is absolutepath to a file - we want the parent folder. Go up one level...
                    if (path.indexOf("/") > 0) {
                        path = path.substring(0, path.lastIndexOf("/"));
                    }

                } else if (null != iDocument.getFieldValue(f)) {
                    // this is the folder path - use all of it
                    path = iDocument.getFieldValue(f);
                }
            } catch (BadMetaException bme){
                logger.debug("Tried to split hierarchical field " + f + " but iDoc has no meta of that name");
            }
            for (String fr : this.buildHierarchicalFacet(path, "/")) {
                // send as _hier_filepath: 0/TrimGroups  , _hier_filepath: 1/TrimGroups/toplevel folder , _hier_filepath: 2/TrimGroups/toplevel folder/another folder
                iDocument.addField("_hier_" + f, fr);
            }
        }

        return iDocument;

    }

    @Override
    public boolean initialiseMe() throws BadConfigurationException {
        if (getIsPropertiesSet()) {
            for (String k : properties.keySet()) {
                if (k.equalsIgnoreCase(CONFIG_HIER_METANAMES)) {
                    String metaNames = properties.get(k);
                    if (metaNames.indexOf(",")>0){
                        String[] metaNamesArray = metaNames.split(",");
                        for (int i = 0; i < metaNamesArray.length; i++){
                            this.metaNames.add(metaNamesArray[i]);
                        }
                    } else {
                        this.metaNames.add(metaNames);
                    }
                }
            }
        }
        if (metaNames.size() == 0){
            throw new BadConfigurationException("HierarchicalFieldSplitter needs at least one meta name to work on - no metas supplied in config as \"hier_metanames\"");
        }
        return true;
    }


    /**
     * Converts a single field that has a hierarchy into a series of fragments that can be passed to Solr and used to create facets
     * eg  D:/folder1/folder2/folder3/file.txt
     * In solr, this should generate facets for D:/, D:/folder1, D:/folder1/folder2 and D:/folder1/folder2/folder3
     * @param _field field to use to create a hierarchical facet in Solr
     * @param _separator character to split the _field input string
     * @return a List of numbered string fragments that can be used in Solr as a hierarchical facet
     */
    public static List<String> buildHierarchicalFacet(String _field, String _separator){

        List<String>retVals = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int counter = 0;

        if (null != _field){
            String input = _field.replaceAll("\"","");
            // special case : field starting file:// or smb://
            if (input.startsWith("file://")){
                input = input.substring(7);
            } else if (input.startsWith("smb:")){
                input = input.substring(6);
            }
            // Now split the string on the separator character(s)
            String[] frags = input.split(_separator);
            for (int i = counter; i < frags.length; i++){
                sb.append(SOLR_CONSTANTS.SOLR_HIERARCHICALFACET_SEPARATOR+frags[i].replaceAll(SOLR_CONSTANTS.SOLR_HIERARCHICALFACET_SEPARATOR,"%2F"));
                retVals.add(i+sb.toString()+SOLR_CONSTANTS.SOLR_HIERARCHICALFACET_SEPARATOR);
            }
        }
        return retVals;
    }



}
