package com.dahu.vector.UnitTests;

import com.dahu.vector.indexers.solr.DefaultSolrFieldMapper;
import com.dahu.vector.processors.HierarchicalFieldSplitter;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by :
 * Vince McNamara, Dahu
 * vince@dahu.co.uk
 * on 18/06/2019
 * copyright Dahu Ltd 2019
 * <p>
 * Changed by :
 */

public class TestSolrHierarchicalFacets {

    @Test
    public void testFacet(){


        String field = "D:\\folder\\subfolder\\more \\another folder here\\long folder with & in the name";
        System.out.println("Building hierarchical facets from " + field);
        List<String> fragments  = HierarchicalFieldSplitter.buildHierarchicalFacet(field,"\\\\");
        Assert.assertTrue(fragments.size() == 6);
        Assert.assertTrue(fragments.get(5).equalsIgnoreCase("5/D:/folder/subfolder/more /another folder here/long folder with & in the name/"));

        field = "smb://didier.office.dahu.co.uk/testdocs/another folder here/long folder with & in the name";
        System.out.println("Building hierarchical facets from " + field);
        fragments = null;
        fragments  = HierarchicalFieldSplitter.buildHierarchicalFacet(field,"/");
        Assert.assertTrue(fragments.size() == 4);
        Assert.assertTrue(fragments.get(3).equalsIgnoreCase("3/didier.office.dahu.co.uk/testdocs/another folder here/long folder with & in the name/"));

    }

}
