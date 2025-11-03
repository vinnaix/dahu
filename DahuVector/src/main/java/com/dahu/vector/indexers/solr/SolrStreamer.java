package com.dahu.vector.indexers.solr;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 22/07/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class SolrStreamer {


    private URL solrUrl = null;
    private String index = null;


    public SolrStreamer(String _solrHostPost, String _index){
        this.index = _index;
        try {
            this.solrUrl = new URL("http://" + _solrHostPost + "/solr/" + _index + "/stream");
        } catch (MalformedURLException murle){
            // strange - this should always look like a Url as we roll it ourselves
            System.out.println("Warn : bad URL " + "http://" + _solrHostPost + "/solr/" + _index + "/stream");
        }
    }


    /**
     * Open a Stream from SOlr and return as an InputStream
     * Requires query to determine which documents to include, and comma-separated list of fields to retrieve
     *
     * @param _query Solr Query
     * @param _fields comma-separated list of Solr queries
     * @return
     */
    public InputStream stream(String _query, String _fields) throws SolrException{


        //        this.streamQuery = "expr=search(" + _index + ",q=\"" + _query + "\",fl=\"" + _fields + "\",sort=\"guid asc\",qt=\"/export\")";
        String streamQuery = "expr=search(" + this.index + ",q=\"" + _query + "\",fl=\"" + _fields + "\",sort=\"id asc\",qt=\"/export\")";


        try {
            HttpURLConnection con = (HttpURLConnection) solrUrl.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(streamQuery);
            wr.flush();
            wr.close();
            int responseCode = con.getResponseCode();
            if (responseCode == 200){
                return con.getInputStream();
            } else {
                throw new SolrException("Solr returned Response code = " + responseCode);
            }

        } catch (IOException ioe){
            throw new SolrException("Solr Error : " + ioe.getLocalizedMessage());
        }
    }




}
