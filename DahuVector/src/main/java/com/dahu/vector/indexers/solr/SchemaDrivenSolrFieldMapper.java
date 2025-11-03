package com.dahu.vector.indexers.solr;

import com.dahu.core.utils.JsonUtils;
import com.dahu.def.exception.BadConfigurationException;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.util.*;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 02/10/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 * Adds support for field-mapping via config
 *
 * Config file defines field/meta names in iDoc and the target field in Solr
 *
 * {
 *   "iDoc2Solr":[
 *     {"idocField":"initialauthor","solrField":"author","multi":true},
 *     {"idocField":"domain","solrField":"domain","multi":false},
 *     {"idocField":"libraryid","solrField":"libraryid","multi":false},
 * }
 *
 * idocField = identifies the name of a field stored in the iDoc METAS container
 * solrField = name of a field defined in Solr schema.
 * multi = true/false depending on whether the field may contain multiple values in the METAS container
 *
 * Note - this mapping does not check for compatible types in the data that is sent to Solr for indexing into
 * the name field. All values are sent to Solr as Strings, and Solr will attempt to cast as appropriate
 * Also, the mapping does NOT check that the solrField corresponds to a valid field in any particular Solr schema.
 * This mapping is NOT tied to any valid Solr Schema, so it is the responsibility of the application owner to
 * ensure that this mapping table is valid for any given Solr schema.
 */

public class SchemaDrivenSolrFieldMapper extends DefaultSolrFieldMapper implements SolrFieldMapper {

    private static final String CONFIG_iDoc2Solr = "iDoc2Solr";
    private static final String CONFIG_IDOCFIELD = "idocField";
    private static final String CONFIG_SOLRFIELD = "solrField";
    private static final String CONFIG_DYNAMICSOLRFIELDTYPE = "dynamicSolrFieldType";
    private static final String CONFIG_MULTI = "multi";

    // keys are valid values that can be put in a SolrSchemaConfig files
    // valid values are the wildcarded types defined in solr schema.xml
    //  <dynamicField name="*_t" type="text_general" indexed="true" stored="true"/>
    //  <dynamicField name="*_s" type="string" indexed="false" stored="true" docValues="true" multiValued="true"/>
    //	<dynamicField name="*_i" type="int" indexed="true" stored="true" multiValued="true" />
    //	<dynamicField name="*_date" type="pdate" indexed="true" stored="true" multiValued="false"/>

    private static final Map<String,String> solrDynamicTypes = new HashMap<String,String>(){
        {
            put("text","t");
            put("string","s");
            put("int","i");
            put("date","date");
        }
    };


    public SchemaDrivenSolrFieldMapper(String _solrSchemaMapFile) throws BadConfigurationException, IOException {
        super();


        JsonNode root = JsonUtils.getConfigAsTree(_solrSchemaMapFile);
        JsonNode idoc2solr = root.get(CONFIG_iDoc2Solr);
        if (idoc2solr.isArray()) {
            Iterator<JsonNode> iter = idoc2solr.iterator();
            while (iter.hasNext()) {
                String idocField = null;
                String solrField = null;
                String dynamicSolrFieldType = null;
                boolean isMulti = false;

                JsonNode idoc2solrNode = iter.next();
                idocField = idoc2solrNode.get(CONFIG_IDOCFIELD).getTextValue();
                if (null != idoc2solrNode.get(CONFIG_SOLRFIELD)){
                    solrField = idoc2solrNode.get(CONFIG_SOLRFIELD).getTextValue();
                } else if (null != idoc2solrNode.get(CONFIG_DYNAMICSOLRFIELDTYPE)){
                    dynamicSolrFieldType = idoc2solrNode.get(CONFIG_DYNAMICSOLRFIELDTYPE).getTextValue();
                }
                if (null != idoc2solrNode.get(CONFIG_MULTI)) {
                    isMulti = idoc2solrNode.get(CONFIG_MULTI).getBooleanValue();
                }

                if (null == idocField){
                    throw new BadConfigurationException("idocField NOT FOUND");
                } else {
                    if (null != solrField){
                        // static field mapping - fixed name in iDoc maps to fixed name in Solr
                        iDoc2Solr2fieldMap.put(idocField, solrField);
                    } else if (null != dynamicSolrFieldType && dynamicSolrFieldType.equalsIgnoreCase("true")){
                        if (idocField.endsWith("*")){
                            idocField = idocField.substring(0,idocField.length()-1);
                        }
                        iDoc2SolrDynamicFields.add(idocField);
                    } else {
                        throw new BadConfigurationException("idocField, " + idocField + " has neither solrField nor dynamicSolrFieldType set");
                    }
                    if (isMulti){
                        fieldsSupportingMultiValues.add(solrField);
                    }
                }
            }
        }
    }

}
