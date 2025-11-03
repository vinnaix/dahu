package com.dahu.aodocs.types;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 18/11/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class AODLibrary {


    private static final String LIBRARYID = "libraryId";
    private static final String NAME = "name";
    private static final String DOMAINNAME = "domainName";
    private static final String STORAGEADMIN = "storageAdmin";
    private static final String DOCUMENTTYPES = "documentTypes";
    private static final String ITEMS = "items";

    private String libraryID;
    private String libraryName;
    private String domainName;
    private String storageAdmin;
    private Set<AODdocumenttype> docTypes = new HashSet<>();

    public AODLibrary(JsonNode _node){

        this.libraryID = _node.get(LIBRARYID).getTextValue();
        this.libraryName = _node.get(NAME).getTextValue();
        this.domainName = _node.get(DOMAINNAME).getTextValue();
        this.storageAdmin = _node.get(STORAGEADMIN).getTextValue();
        JsonNode documentTypes = _node.get(DOCUMENTTYPES);
        if (null != documentTypes && documentTypes.isObject()) {
            JsonNode items = documentTypes.get(ITEMS);
            if (null != items && items.isArray()) {

                for (int i = 0; i < ((ArrayNode) items).size(); i++) {
                    JsonNode item = ((ArrayNode) items).get(i);
                    AODdocumenttype docType = new AODdocumenttype(item);
                    docTypes.add(docType);
                }
            }
        }
    }

    public String getLibraryID() {
        return libraryID;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public String getDomainName() {
        return domainName;
    }

    public String getStorageAdmin() {
        return storageAdmin;
    }

    public Set<AODdocumenttype> getDocTypes() {
        return docTypes;
    }

    public JsonNode getJson(){

        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put(LIBRARYID,this.libraryID);
        node.put(NAME,this.libraryName);
        node.put(DOMAINNAME,this.domainName);
        node.put(STORAGEADMIN,this.storageAdmin);

        ObjectNode docTypesNode = node.putObject(DOCUMENTTYPES);
        ArrayNode itemsNode = docTypesNode.putArray(ITEMS);
        for (AODdocumenttype docType : docTypes){
            itemsNode.add(docType.getJson());
        }

        return node;
    }



}
