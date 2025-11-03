package com.dahu.vector.indexers.solr;

import org.jetbrains.annotations.NotNull;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 23/07/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class SolrException extends Exception {

    public SolrException(){ super();}

    public SolrException(@NotNull String arg){super(arg);}
}
