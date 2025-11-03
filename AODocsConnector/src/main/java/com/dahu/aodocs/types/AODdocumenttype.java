package com.dahu.aodocs.types;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import static com.dahu.core.document.DOCUMENT_CONSTANTS.FIELDNAME_ID;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 18/11/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class AODdocumenttype {

    private String docTypeID;
    private String docTypeName;

    public AODdocumenttype(JsonNode _node){

        this.docTypeID = _node.get("value").getTextValue();
        this.docTypeName = _node.get("name").getTextValue();
    }

    public String getDocTypeID() {
        return docTypeID;
    }

    public String getDocTypeName() {
        return docTypeName;
    }

    public JsonNode getJson(){
        // get all the metadata for this document
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);

        // These METAs are required to exist and have non-null values so we always copy them
        node.put("name",this.docTypeName);
        node.put("value",this.docTypeID);
        return node;
    }

}
