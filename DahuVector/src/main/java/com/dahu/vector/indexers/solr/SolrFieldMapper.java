package com.dahu.vector.indexers.solr;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 02/10/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public interface SolrFieldMapper {


    public String getSolrFieldName(String _iDocFieldName);

    public boolean isMulti(String _iDocFieldName);

    public boolean isDynamicField(String _iDocFieldName);

}
