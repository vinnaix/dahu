package com.dahu.plugins.edge.jdbc.storage;

import com.dahu.def.exception.BadArgumentException;
import com.dahu.def.exception.BadConfigurationException;
import com.dahu.def.exception.ContextException;
import com.dahu.def.plugins.StorePluginBase;
import com.dahu.def.types.Store;
import com.dahu.plugins.edge.jdbc.connection.JDBCConnection;
import org.apache.logging.log4j.Level;

import java.io.UnsupportedEncodingException;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 29/05/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class JDBCConnectionStorage extends StorePluginBase<JDBCConnection> {


    public JDBCConnectionStorage(Level _level, Store _plugin){
        super(_level, _plugin);



    }


    @Override
    public void doStartup(Store store) throws ContextException, BadArgumentException, BadConfigurationException, UnsupportedEncodingException {

    }

    @Override
    public void doShutdown(Store store) {

    }

    @Override
    public void doRefresh(Store store) {

    }

    @Override
    public JDBCConnection getStore() {
        return null;
    }
}
