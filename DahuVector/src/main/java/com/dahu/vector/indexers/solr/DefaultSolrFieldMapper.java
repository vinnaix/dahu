package com.dahu.vector.indexers.solr;

import com.dahu.vector.core.processors.TIKA_CONSTANTS;

import java.util.*;

import static com.dahu.vector.indexers.solr.SOLR_CONSTANTS.*;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 26/09/2017
 * copyright Dahu Ltd 2017
 * <p>
 * Changed by :
 *
 *
 * DEFINITION OF SOLR FIELDS
 *
 * Our Solr Config will not change (very often)
 * This is the set of fields we want to try to write to in Solr
 *
 * For each one, we should try to find a good field from the iDoc, probably populated by Tika but possibly from UIMA or other processors
 *
 * Some values exist as toplevel fields in an iDoc - most will be passed in the METAS contained in the iDoc
 *
 * If the Solr schema supports dynamic fields, then there are patterns of field names in the iDoc that should be sent
 * to Solr to match a dynamic field definition
 *
 * eg let this define a dynamic field pattern in Solr schema
 *
 * aodocs_str_*   // any field coming from AODocs metadata that should be treated as a String type (not tokenised)
 *
 * Let's define this mapping from iDoc to Solr
 *
 * iDoc2SolrDynamicFieldMap.put(aodocs_person, aodocs_str);
 *
 * Now, if there is a METAS field that comes from AODocs where the field name is "aodocs_person",
 * this SolrCloudIndexer will send a field NVP to Solr with this format
 *
 * Solr field name = aodocs_str_aodocs_person
 * value = value for the aodocs_person META field
 *
 */

public class DefaultSolrFieldMapper implements SolrFieldMapper {

    protected Map<String,String> iDoc2Solr2fieldMap = new HashMap<>(); // static mapping from iDoc field to Solr field
    protected Set<String> fieldsSupportingMultiValues = new HashSet<>(); // Set of Solr fields that support multi-values
    protected Set<String> iDoc2SolrDynamicFields = new HashSet<>(); // field name prefixes in iDoc that should map to dynamic solr fields


    public DefaultSolrFieldMapper(){

        // ALL THESE FIELDS COME FROM THE METADATA OBJECT OF AN iDOC
        iDoc2Solr2fieldMap.put(TIKA_CONSTANTS.TIKAFIELD_AUTHOR,SOLRFIELD_AUTHOR);  // list of content authors
        iDoc2Solr2fieldMap.put("owner",SOLRFIELD_OWNER);   // content owner for FS; Mailbox owner for Email; OneDrive primary account holder; not defined for other types
        iDoc2Solr2fieldMap.put("filename",SOLRFIELD_FILENAME);    // name of item including extension for file . related to physical location
        iDoc2Solr2fieldMap.put("filepath",SOLRFIELD_FILEPATH);    // path to item from FS
        iDoc2Solr2fieldMap.put("sourcename",SOLRFIELD_SOURCE);      // EDGE Connector name
        iDoc2Solr2fieldMap.put("servername",SOLRFIELD_SERVERNAME);      // canonical name of server that hosts the content if available
        iDoc2Solr2fieldMap.put("sharename",SOLRFIELD_SHARENAME);       // for cifs the Sharename
        iDoc2Solr2fieldMap.put("ext",SOLRFIELD_EXT);     // file extension if available - the file name after the last "." char - only valid for stored content eg FS, OneDrive
        iDoc2Solr2fieldMap.put(TIKA_CONSTANTS.TIKAFIELD_TO,SOLRFIELD_TO);      // only valid for EMAIL - list of names & email addresses for recipient
        iDoc2Solr2fieldMap.put(TIKA_CONSTANTS.TIKAFIELD_FROM,SOLRFIELD_FROM);    // only valid for EMAIL - single name & email address for sender
        iDoc2Solr2fieldMap.put(TIKA_CONSTANTS.TIKAFIELD_CC,SOLRFIELD_CC);      // only valid for EMAIL - list of names & email addresses for COPY
        iDoc2Solr2fieldMap.put(TIKA_CONSTANTS.TIKAFIELD_KEYWORDS,SOLRFIELD_KEYWORDS); // general-purpose text field for high value content eg Subject, Description, Keywords
        iDoc2Solr2fieldMap.put(TIKA_CONSTANTS.TIKAFIELD_CREATED_DATE,SOLRFIELD_CREATED_DATE); // Created date as timestamp - long
        iDoc2Solr2fieldMap.put(TIKA_CONSTANTS.TIKAFIELD_CREATED_DATE_STR,SOLRFIELD_CREATED_DATE_STR); // Created date as String in Zulu format
        iDoc2Solr2fieldMap.put("checksum",SOLRFIELD_CHECKSUM);    // value calculated by Connector for content - to be used for DeDupe
        iDoc2Solr2fieldMap.put("simhash","simhash");     // value calculated by Connector for content - to be used for Near DeDupe
        iDoc2Solr2fieldMap.put("acl","acl"); // Assumes we can package all the ACL data into a single field
        iDoc2Solr2fieldMap.put("allow_token_document","allow_token_document"); // Assumes we can package all the ACL data into a single field
        iDoc2Solr2fieldMap.put("allow_token_parent","allow_token_parent"); // from Datafari - do we need? maybe backwards compatible
        iDoc2Solr2fieldMap.put("allow_token_share","allow_token_share"); // from Datafari - do we need? maybe backwards compatible
        iDoc2Solr2fieldMap.put("deny_token_document","deny_token_document"); // from Datafari - do we need? maybe backwards compatible
        iDoc2Solr2fieldMap.put("deny_token_parent","deny_token_parent"); // from Datafari - do we need? maybe backwards compatible
        iDoc2Solr2fieldMap.put("deny_token_share","deny_token_share"); // from Datafari - do we need? maybe backwards compatible


        fieldsSupportingMultiValues.add(SOLRFIELD_METAS);
        fieldsSupportingMultiValues.add(SOLRFIELD_AUTHOR);
        fieldsSupportingMultiValues.add(SOLRFIELD_OWNER);
        fieldsSupportingMultiValues.add(SOLRFIELD_ALLOW_TOKEN_DOCUMENT);
        fieldsSupportingMultiValues.add(SOLRFIELD_ALLOW_TOKEN_PARENT);
        fieldsSupportingMultiValues.add(SOLRFIELD_ALLOW_TOKEN_SHARE);
        fieldsSupportingMultiValues.add(SOLRFIELD_ENTITY);
        fieldsSupportingMultiValues.add(SOLRFIELD_TO);
        fieldsSupportingMultiValues.add(SOLRFIELD_FROM);
        fieldsSupportingMultiValues.add(SOLRFIELD_DESCRIPTION);
        fieldsSupportingMultiValues.add(SOLRFIELD_SUBJECT);
        fieldsSupportingMultiValues.add(SOLRFIELD_KEYWORDS);
        fieldsSupportingMultiValues.add(SOLRFIELD_CHILD_IDS);


    }


    /**
     * Test whether a field in Solr schema supports multiple values
     * @param _fieldName name of field as defined in solr index schema
     * @return true if the field supports multi-values in standard Dahu  SOLR schema
     */
    public boolean isMulti(String _fieldName){

        if (_fieldName.startsWith(SOLR_CONSTANTS.SOLR_HIERARCHICALFACET_PREFIX)){
            return true;
        } else {
            return fieldsSupportingMultiValues.contains(_fieldName);
        }
    }


    /**
     * Convert from iDoc field name to equivalent Solr one
     * Returns null if there is no Solr field for this iDoc field
     * @param _iDocFieldName, name of a field in an iDoc
     * @return  equivalent field name in our Solr index for this field
     */
    public String getSolrFieldName(String _iDocFieldName){

        // Special case - any iDoc fields that start _hier_* are always treated as Solr Fields
        if (_iDocFieldName.startsWith(SOLR_CONSTANTS.SOLR_HIERARCHICALFACET_PREFIX)){
            return _iDocFieldName;
        } else {
            return iDoc2Solr2fieldMap.get(_iDocFieldName);
        }
    }

    /**
     * Does this field name match any of the permitted patterns for dynamic fields?
     * If yes, then we want to send this field to Solr and allow Solr's dynamic schema to match the field
     * @param _iDocFieldName a field name in iDoc
     * @return true if there is a matching prefix for this field name indicating that it will match a Solr wild-carded field name
     */
    public boolean isDynamicField(String _iDocFieldName){
        for (String s : iDoc2SolrDynamicFields){
            if (_iDocFieldName.startsWith(s)){
                return true;
            }
        }
        return false;
    }

}
