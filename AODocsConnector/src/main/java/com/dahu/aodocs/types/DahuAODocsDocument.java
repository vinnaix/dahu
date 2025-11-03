package com.dahu.aodocs.types;

import com.dahu.aodocs.AODOCS_CONSTANTS;
import com.dahu.aodocs.APIservices.documentIdAPI;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.type.TypeReference;

import java.util.*;

import static com.dahu.aodocs.AODOCS_CONSTANTS.CONFIG_AODOCS_STORAGE;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 05/08/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 * Container class for gathering and normalising the metadata from a document in AODocs
 *
 * Retrieves standard metadata elements from AODocs search API, including Id, Title, Last-modified etc
 *
 * Retrieves Folder as a hierarchical folderpath, separated by "/" characters.
 *
 * Reads custom Fields from AODocs
 * Reads custom Categories from AODocs. A Category is a custom field that contains an enumarated set of possible values.
 * Categories could contain multiple values per document. The can also contain Hierarchical values - these are multiple values
 * where each value should logically be "appended" to the previous value to create a chain. For a Hierarchical category, the
 * fields are essentially concatenated in the right order and flattened to create a structure as a/b/c
 *
 * eg category element 1 - name = "a"  parentId = "0" Id = '123"
 * category element 2 - name = "b" parentId = "123" Id = "456"
 * category element 3 - name = "c" parentId = "456" Id = "789"
 *
 * We stitch these three elements together based on their parent-child relations, and flatten to treat as single value, "a/b/c"
 *
 * AODocs permissions are retrieved and flattened into an array of ROLE:USERNAME values
 *
 * There is a toJson() method, that turns an AODocsDocument into a Json (String) representation that can be pushed to a queue.
 * Vector can read the Json as a text message from the queue, and pass to a constructor of an iDoc
 * Vector should then use an AODocsFetchInjector to get the raw document bytes from a call to the G-Drive API before processing in Tika.
 *
 */

public class DahuAODocsDocument {

    // standard fields
    private String domain = null;
    private String docId = null;
    private String libraryId = null;
    private String libraryName = null;
    private String libraryStorageAccount = null;
    private String title = null;
    long creationDate = 0;
    long modificationDate = 0;

    private Map<String,String> fields = new HashMap<>();

    // folders
    private Set<AODocsFolder> folders = new HashSet<>(); // all folders, in random order
    private List<String> folderPath = new ArrayList<>(); // all folders in order from root
    private String folderPathStr = null;

    //attachments
    private Set<AODocsDocumentAttachment> attachments = new HashSet<>();
    // permissions
    private Set<AODocsPermission> permissions = new HashSet<>();
    //fields
    private Set<AODocsField> customFields = new HashSet<>();
    // categories
    private Set<AODocsCategory> categories = new HashSet<>();
    // Comments
    private List<AODComment> comments = new ArrayList<>();

    private documentIdAPI docIdAPI = null; // AODocs API Service to look up Comments for a document


    public DahuAODocsDocument(JsonNode _node){

        // For testing purposes, the minimum set of fields is "domainName" and "id"
        // this is sufficient for us to create a simple AODocsDocument so that we can test reading Comments from AODocs
        domain = _node.get("domainName").getTextValue();
        docId = _node.get("id").getTextValue();

        // For a real response from AODocs, there are a bunch of mandatory AODocs fields
        if (null != _node.get("libraryId")) { // just test we are dealing with a real doc, not one created for our Comments test
            for (String fName : AODOCS_CONSTANTS.defaultFieldNames){
                if (null != _node.get(fName)){
                    fields.put(fName.toLowerCase(),_node.get(fName).getTextValue());
                }
            }
            libraryName = _node.get("libraryName").getTextValue();
            libraryId = _node.get("libraryId").getTextValue();
            title = _node.get("title").getTextValue();

            // Link to AODocs interface for this document
            String aoDocsViewUrl = "https://aodocs.altirnao.com/?locale=en_US&aodocs-domain=" + domain +
                    "#Menu_viewDoc/LibraryId_" + libraryId +
                    "/DocumentId_" + docId;

            fields.put("aodocsurl",aoDocsViewUrl);

            String creationDateStr = _node.get("creationDate").getTextValue();
            String modificationDateStr = _node.get("modificationDate").getTextValue();
            try {
                creationDate = Long.parseLong(creationDateStr);
                modificationDate = Long.parseLong(modificationDateStr);
            } catch (NumberFormatException nfe) {
                // do nothing
            }

            //  FOLDERS
            if (null != _node.get("folders")) {
                // From the Folders structure returned for an AODocs document, we should be able to say which is a root folder and which is one below root
                // For all the others, we can work out the structure by looking at parent-child relationships but only after we get the full set and can traverse up and down
                AODocsFolder rootFolder = null;
                AODocsFolder firstFolder = null;

                if (_node.get("folders").isArray()) {
                    for (int i = 0; i < ((ArrayNode) _node.get("folders")).size(); i++) {
                        JsonNode folderNode = ((ArrayNode) _node.get("folders")).get(i);
                        AODocsFolder f = new AODocsFolder(folderNode);
                        folders.add(f);
                        if (f.isRoot()) {
                            rootFolder = f;
                        }
                        if (f.parentIsRoot()) {
                            firstFolder = f;
                        }
                    }
                }

                // Lets try to build up a path for the entire folder structure by traversing up/down
                // start with the root folder
                folderPath.add(rootFolder.getName());
                int numberInCorrectOrder = 1;
                String trailingFolderAodocsId = rootFolder.getFolderAodocsId();// Start with the root folder

                // Now is there another one?
                if (null != firstFolder) {
                    // There's at least one more folder
                    folderPath.add(firstFolder.getName());
                    numberInCorrectOrder = 2;
                    trailingFolderAodocsId = firstFolder.getFolderAodocsId();
                }

                // Find the folder that has our current trailing Folder as its parent Id and stick it on the end of our folder path
                while (numberInCorrectOrder < folders.size()) {
                    for (AODocsFolder f : folders) {
                        if (null != f.getParentFolderAodocsId() && f.getParentFolderAodocsId().equalsIgnoreCase(trailingFolderAodocsId)) {
                            folderPath.add(f.getName());
                            trailingFolderAodocsId = f.getFolderAodocsId();
                            numberInCorrectOrder++;
                        }
                    }
                }
                // Now turn that linked list of folders into a string path
                // use '/' char as separator, and escape any '/' chars that exist in the folder name
                StringBuilder folderPathSB = new StringBuilder();
                for (String s : folderPath) {
                    folderPathSB.append(s.replaceAll("/", "%2F") + "/");
                }
                folderPathSB.deleteCharAt(folderPathSB.length() - 1);
                folderPathStr = folderPathSB.toString();
            }

            // END FOLDERS

            // PERMISSIONS
            if (null != _node.get("permissions") && _node.get("permissions").isArray()) {
                for (int i = 0; i < ((ArrayNode) _node.get("permissions")).size(); i++) {
                    JsonNode permissionsNode = ((ArrayNode) _node.get("permissions")).get(i);
                    permissions.add(new AODocsPermission(permissionsNode));
                }
            } else {
                // If there are no specific restrictions on th document, then give it an ACL entry of EVERYONE
                permissions.add(new AODocsPermission(true));
            }
            // END PERMISSIONS

            // ATTACHMENTS
            if (null != _node.get("attachments") && _node.get("attachments").isArray()) {
                for (int i = 0; i < ((ArrayNode) _node.get("attachments")).size(); i++) {
                    JsonNode attachmentsNode = ((ArrayNode) _node.get("attachments")).get(i);
                    attachments.add(new AODocsDocumentAttachment(attachmentsNode));
                }
            }
            // END ATTACHMENTS

            // CUSTOM FIELDS
            if (null != _node.get("fields") && _node.get("fields").isArray()) {
                for (int i = 0; i < ((ArrayNode) _node.get("fields")).size(); i++) {
                    JsonNode fieldNode = ((ArrayNode) _node.get("fields")).get(i);
                    AODocsField field = new AODocsField(fieldNode);
                    if (null != field && null != field.getFieldName() && null != field.getValues() && field.getValues().size() > 0) {
                        customFields.add(field);
                    }
                }
            }
            // END CUSTOM FIELDS

            // CATEGORIES
            if (null != _node.get("categories") && _node.get("categories").isArray()) {
                for (int i = 0; i < ((ArrayNode) _node.get("categories")).size(); i++) {
                    JsonNode categoryNode = ((ArrayNode) _node.get("categories")).get(i);
                    AODocsCategory category = new AODocsCategory(categoryNode);
                    if (null != category && null != category.getCategoryName()
                            && null != category.getValues()
                            && category.getValues().size() > 0
                            && !category.getCategoryName().equalsIgnoreCase("FOLDER")
                            ) {
                        categories.add(category);
                    }
                }
            }
            // END CATEGORIES

        }

    }


    public Set<AODocsDocumentAttachment> getAttachments() {
        return attachments;
    }

    public Set<AODocsPermission> getPermissions() {
        return permissions;
    }

    public Set<AODocsField> getCustomFields(){ return customFields; }

    public Set<AODocsCategory> getCategories(){ return categories;}

    public long getCreationDate() {
        return creationDate;
    }

    public long getModificationDate() {
        return modificationDate;
    }

    public String getFolderPathStr() { return folderPathStr; }

    public String getDomain() { return domain; }

    public String getLibraryName(){return this.libraryName;}

    public String getDocId(){ return this.docId; }

    public String getTitle(){return this.title;}

    public Map<String,String> getFields(){return this.fields;}

    public List<AODComment> getComments(){return this.comments;}

    public void addComment(AODComment _comment){ this.comments.add(_comment);}

    public void setStorageAccount(String _user){this.libraryStorageAccount = _user;}

    public String getStorageAccount(){ return this.libraryStorageAccount;}

    public ObjectNode getJson(){

        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put("metas_docid",docId);
        node.put("metas_domain",domain);
        node.put("metas_title",title);
        node.put("metas_libraryname",libraryName);
        node.put("metas_libraryid",libraryId);
        node.put("metas_creationdate",creationDate);
        node.put("last_modified_date",modificationDate);
        node.put("metas_folderpath",folderPathStr);
        node.put("metas_"+CONFIG_AODOCS_STORAGE,this.libraryStorageAccount);
        for (String k : fields.keySet()){
            node.put("metas_"+k,fields.get(k));
        }

        return node;
    }

    public class AODocsPermission {

        String type;
        String value;
        String right;
        String name;
        String thumbnailPhotoUrl;
        List<String> roleNames = new ArrayList<>();
        boolean isReader = false;

        private AODocsPermission(JsonNode _node){
            if (null != _node.get("type")) type = _node.get("type").getTextValue();
            if (null != _node.get("value")) value = _node.get("value").getTextValue();
            if (null != _node.get("right")) right = _node.get("right").getTextValue();
            if (null != _node.get("name")) name = _node.get("name").getTextValue();
            if (null != _node.get("thumbnailPhotoUrl")) thumbnailPhotoUrl = _node.get("thumbnailPhotoUrl").getTextValue();

            if  (null != _node.get("roleNames") && _node.get("roleNames").isArray()){
                try {
                    roleNames = new ObjectMapper().readValue(_node.get("roleNames"), new TypeReference<ArrayList<String>>() {});
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
            for (String s : roleNames){
                if (s.equals(AODOCS_CONSTANTS.PERMISSION_READERS)){
                    isReader = true;
                    break;
                }
            }
        }

        private AODocsPermission(boolean _isEveryone){
            type="USER";
            value="EVERYONE";
            right="NONE";
            name="EVERYONE";
            isReader=true;
        }

        public String getType() {
            return type;
        }

        public String getValue() {
            return value;
        }

        public String getRight() {
            return right;
        }

        public String getName() {
            return name;
        }

        public String getThumbnailPhotoUrl() {
            return thumbnailPhotoUrl;
        }

        public List<String> getRoleNames() {
            return roleNames;
        }

        public ObjectNode getJson(){
            ObjectNode node = new ObjectNode(JsonNodeFactory.instance);

            node.put("type",type);
            node.put("value",value);
            node.put("right",right);
            node.put("name",name);
            node.put("thumbnailPhotoUrl",thumbnailPhotoUrl);

            ArrayNode arrayNode = node.putArray("roleNames");
            for (String r : roleNames){
                arrayNode.add(r);
            }

            return node;
        }

        public boolean isReader(){return isReader;}

    }

    public class AODocsDocumentAttachment{

        String fileId;
        String name;
        String mimeType;
        String link;
        String iconLink;
        int size;

        private AODocsDocumentAttachment(JsonNode _node){

            fileId = _node.get("fileId").getTextValue();
            name = _node.get("name").getTextValue();
            mimeType = _node.get("mimeType").getTextValue();
            link = _node.get("link").getTextValue();
            iconLink = _node.get("iconLink").getTextValue();
            String sizeStr = _node.get("size").getTextValue();
            try {
                size = Integer.parseInt(sizeStr);
            } catch (NumberFormatException nfe){
                size = 0;
            }
        }

        public String getFileId() {
            return fileId;
        }

        public String getName() {
            return name;
        }

        public String getMimeType() {
            return mimeType;
        }

        public String getLink() {
            return link;
        }

        public String getIconLink() {
            return iconLink;
        }

        public int getSize() {
            return size;
        }

        public ObjectNode getJson(){

            ObjectNode node = new ObjectNode(JsonNodeFactory.instance);

            node.put("fileid",fileId);
            node.put("name",name);
            node.put("mimetype",mimeType);
            node.put("link",link);
            node.put("iconlink",iconLink);
            node.put("size",size);

            return node;
        }

    }

    public class AODocsFolder {

        private String folderAodocsId = null;
        private String fileId = null;
        private String parentFolderAodocsId = null;
        private boolean isRoot = false;
        private boolean parentIsRoot = false;
        private String name = null;

        private AODocsFolder(JsonNode _node){
            folderAodocsId = _node.get("folderAodocsId").getTextValue();
            fileId = _node.get("fileId").getTextValue();
            if (null != _node.get("parentFolderAodocsId")){
            parentFolderAodocsId = _node.get("parentFolderAodocsId").getTextValue();
            }
            name = _node.get("name").getTextValue();
            if (null != _node.get("folderIsRoot")){
                isRoot = _node.get("folderIsRoot").getBooleanValue();
            }
            if (null != _node.get("parentFolderIsRoot") ){
                parentIsRoot = _node.get("parentFolderIsRoot").getBooleanValue();
            }
        }

        public String getFolderAodocsId() { return folderAodocsId; }

        public String getFileId() { return fileId; }

        public String getParentFolderAodocsId() { return parentFolderAodocsId; }

        public boolean isRoot() { return isRoot; }

        public boolean parentIsRoot(){ return parentIsRoot; }

        public String getName() { return name; }

        public ObjectNode getJsonNode(){
            // get all the metadata for this document
            ObjectNode node = new ObjectNode(JsonNodeFactory.instance);

            node.put("folderaodocsid",folderAodocsId);
            node.put("fileid",fileId);
            node.put("parentfolderaodocsid",parentFolderAodocsId);
            node.put("name",name);

            return node;
        }

    }


    public class AODocsField {

        private String fieldId;
        private String fieldName;
        private String type;
        private List<String> values = null;

        private AODocsField(JsonNode _node){
            fieldId = _node.get("fieldId").getTextValue();
            fieldName = _node.get("fieldName").getTextValue();
            type = _node.get("type").getTextValue();

            if (null != _node.get("values") && _node.get("values").isArray()){
                try {
                    values = new ObjectMapper().readValue(_node.get("values"), new TypeReference<ArrayList<String>>() {});
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

        public String getFieldId() {
            return fieldId;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String getType() {
            return type;
        }

        public List<String> getValues() {
            return values;
        }

        public ObjectNode getJson(){
                ObjectNode node = new ObjectNode(JsonNodeFactory.instance);

                node.put("type",type);
                node.put("id",fieldId);
                node.put("name",fieldName);

                ArrayNode arrayNode = node.putArray("fieldValues");
                for (String r : values){
                    arrayNode.add(r);
                }

                return node;
            }

        }


    /**
     * Container for AODocs Category fields
     * AODocs categories can be multi-valued - ie you can set multiple strings
     * AODocs categories can also be hierarchical. We want to flatten the hierarchical ones into a single value
     * with a '/' char as separator
     */
    public class AODocsCategory {

        private String categoryId;
        private String categoryName;
        private List<String> values = new ArrayList<>();

        private AODocsCategory(JsonNode _node){
            categoryId = _node.get("fieldId").getTextValue();
            categoryName = _node.get("fieldName").getTextValue();

            if (null != _node.get("values") && _node.get("values").isArray()){
                for (int i = 0; i < ((ArrayNode)_node.get("values")).size(); i++){
                    JsonNode categoryValue = ((ArrayNode)_node.get("values")).get(i);
                    // If this is arraynode, then there are multiple values
                    // They may or may not be hierarchical
                    if (null != categoryValue && categoryValue.isArray()){
                        for (int j = 0; j < categoryValue.size(); j++){
                            JsonNode childCategoryValue = categoryValue.get(j);
                            String categoryValueName = childCategoryValue.get("name").getTextValue();
                            String parentId = childCategoryValue.get("parentId").getTextValue();

                            if (null != categoryValueName && null != parentId){
                                if (parentId.equalsIgnoreCase("0")){
                                    values.add(categoryValueName); // NOT Hierarchical OR first element of a Hierarchical category
                                } else {
                                    String replacementValue = values.get(0)+"/"+categoryValueName;
                                    values.remove(0);
                                    values.add(replacementValue); // Its hierarchical so append this value
                                }
                            }
                        }
                    } else {
                        if (null != categoryValue.get("name")){
                            values.add(categoryValue.get("name").getTextValue());
                        }
                    }
                }

            }
        }

        public String getCategoryId() {
            return categoryId;
        }

        public String getCategoryName() {
            return categoryName;
        }

        public List<String> getValues() {
            return values;
        }

        public ObjectNode getJson(){
            ObjectNode node = new ObjectNode(JsonNodeFactory.instance);

            node.put("id",categoryId);
            node.put("name",categoryName);

            ArrayNode arrayNode = node.putArray("categoryvalues");
            for (String r : values){
                arrayNode.add(r);
            }

            return node;
        }

    }



}
