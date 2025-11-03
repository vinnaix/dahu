package com.dahu.google;

import com.dahu.aodocs.AODOCS_CONSTANTS;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 06/08/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 *
 * Stream a document from Google Drive API
 *
 */

public class GoogleDocFetchAPI {

    private HttpTransport httpTransport = new NetHttpTransport();
    private JacksonFactory jsonFactory = new JacksonFactory();

    GoogleCredentials credentials;

    public GoogleDocFetchAPI(GoogleCredentials _creds){
        this.credentials = _creds;
    }

    /**
     * Retrieve the contents of a file stored on Google Drive
     * The method used to stream the content is different depending on whether the file is a G Suite document or
     * a regular file.
     * Credentials provided to the constructor will be used to open a connection over the Google Drive API
     * @param _docId Google Drive file ID
     * @param _mimeType G Drive file mime type
     * @return byte array containing raw document body
     */
    public byte[] fetch(String _docId, String _mimeType){

        if (_mimeType.startsWith("application/vnd.google-apps.")) {
            // need to use the EXPORT API method
            String targetMime = convertToSupportedExportFormat(_mimeType);
            return export(_docId, targetMime,credentials);
        } else {
            // need to use the GET API method
            return fetch(_docId, credentials);
        }
    }


    /**
     * G-Suite doc types cannot be streamed using a simple "get" method. The native format would not be
     * parseable by client applications on the local client, outside of a browser.
     * Instead, G-Suite docs must be "exported" to stream the raw bytes converted to a format that can be
     * consumed on the client. There are a set of export formats that are possible for each G-Suite doc type.
     * Conversion from the G Suite type to an export type is managed internally within this class.
     *
     * @param _docId The G Drive ID for a file to be exported
     * @param _docType G-Suite type. should be one of "document", "spreadsheet", "presentation", "pdf" - derived from the mime-type
     * @param _creds GoogleCredentials object with delegation to an account that has READ access to the documents
     * @return byte array containing the raw data in the preferred export format
     */
    public byte[] export(String _docId, String _docType, GoogleCredentials _creds){

        String docType = null;
        if (null != _docType){
            if (_docType.startsWith("application/vnd.google-apps.")){
                docType = _docType.substring(28);
            } else {
                docType = _docType;
            }
        }
        String exportMimeType = convertToSupportedExportFormat(docType);

        // Adapter needed by Google API to handle GoogleCredentials
        HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(_creds);
        // Google Drive API
        Drive serviceDrive = new Drive.Builder(httpTransport,jsonFactory,requestInitializer)
                .setApplicationName(AODOCS_CONSTANTS.GOOGLE_APP_NAME)
                .build();

        OutputStream outputStream = new ByteArrayOutputStream();
        try {
            serviceDrive.files().export(_docId, exportMimeType).executeMediaAndDownloadTo(outputStream);
        } catch (IOException ioe){
            ioe.printStackTrace();
            return new byte[]{};
        }
        return ((ByteArrayOutputStream) outputStream).toByteArray();

    }

    /**
     * Retrieve a document from Google Drive
     * @param _docId Google Drive API Document ID
     * @param  _creds GoogleCredentials object encapsulating a service account
     * @return byte array containing raw document body
     */
    public byte[] fetch(String _docId, GoogleCredentials _creds){

        // Adapter needed by Google API to handle GoogleCredentials
        HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(_creds);
        // Google Drive API
        Drive serviceDrive = new Drive.Builder(httpTransport,jsonFactory,requestInitializer)
                .setApplicationName(AODOCS_CONSTANTS.GOOGLE_APP_NAME)
                .build();

        OutputStream outputStream = new ByteArrayOutputStream();
        try {
            serviceDrive.files().get(_docId).executeMediaAndDownloadTo(outputStream);
        } catch (IOException ioe){
            ioe.printStackTrace();
            return new byte[]{};
        }
        return ((ByteArrayOutputStream) outputStream).toByteArray();

    }


    /**
     * https://developers.google.com/drive/api/v3/ref-export-formats
     *
     * When exporting a G Suite document, you need to specify the format you want to receive the export in
     * There are defined output formats that are available for different G-Suite formats, as defined in the above document
     * Since we only care about exporting the text for indexing, we can decide on our preferred format for each G Suite type,
     * based on the ease of filtering to extract text
     *
     * @param _mimeType a G Suite type, one of Document, Spreadsheet, PDF, Drawing, Presentation, App script
     * @return a mime type that is compatible with the export function for the given G Suite type
     */
    public static String convertToSupportedExportFormat(String _mimeType){


            if (null != _mimeType && _mimeType.startsWith("application/vnd.google-apps.")) {
                String GSuiteFormat = _mimeType.substring(28);
                switch (GSuiteFormat.toLowerCase()) {
                    case "document":
                        return "text/plain";
                    case "spreadsheet":
                        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
                    case "pdf":
                        return "application/pdf";
                    case "drawing":
                        return "application/pdf";
                    case "appscript":
                        return "application/vnd.google-apps.script+json";
                    default:
                        return "application/pdf";
                }
            } else {
                return _mimeType;
            }
    }

}
