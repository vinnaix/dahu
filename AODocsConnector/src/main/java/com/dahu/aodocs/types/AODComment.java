package com.dahu.aodocs.types;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 07/10/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class AODComment {


    private String authorName;
    private String authorEmail;
    private String createdTime;
    private String lastModTime;
    private String message;

    public AODComment(JsonNode _node){
/*
{
   "kind": "aodocs#comment",
   "id": "ReCW4zH7eDQvkWDnY3",
   "createdTime": "2019-10-07T08:23:53.307Z",
   "modifiedTime": "2019-10-07T08:23:53.307Z",
   "author": {
    "kind": "aodocs#baseProfile",
    "email": "vince@dahu.co.uk",
    "name": "Vince McNamara",
    "thumbnailPictureUrl": "https://lh4.googleusercontent.com/-sj9gPp-ELK0/AAAAAAAAAAI/AAAAAAAAAAA/ACHi3rfw1Ukp5Fe7UJV4VIDPmzy51fMqPg/photo.jpg"
   },
   "message": "there should be more comments"
  }
 */

        this.createdTime = _node.get("createdTime").getTextValue();
        this.lastModTime = _node.get("modifiedTime").getTextValue();
        this.message = _node.get("message").getTextValue();
        JsonNode author = _node.get("author");
        if (null != author && author.isObject()){
            this.authorEmail = author.get("email").getTextValue();
            this.authorName = author.get("name").getTextValue();
        }
    }

    public String getAuthorName() {
        return authorName;
    }

    public String getAuthorEmail() {
        return authorEmail;
    }

    public String getCreatedTime() {
        return createdTime;
    }

    public String getLastModTime() {
        return lastModTime;
    }

    public String getMessage() {
        return message;
    }

    public String toJson(){
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("author",authorName);
        node.put("authorEmail",authorEmail);
        node.put("createdTime",createdTime);
        node.put("modifiedTime",lastModTime);
        node.put("message",message);
        return node.toString();
    }

    public String toString(){
        StringBuilder retVal = new StringBuilder();
        retVal.append(authorName+" _ ");
        retVal.append(authorEmail+" _ ");
        retVal.append(createdTime+" _ ");
        retVal.append(lastModTime+" _ ");
        retVal.append(message);
        return retVal.toString();
    }

}
