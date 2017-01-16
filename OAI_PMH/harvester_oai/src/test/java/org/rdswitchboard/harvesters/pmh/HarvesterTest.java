package org.rdswitchboard.harvesters.pmh;

import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;


public class HarvesterTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private Properties figshareProperties = new Properties();

    @org.junit.Before
    public void setUp() throws Exception {
        File tempFolder = testFolder.newFolder("testHarvest");
        figshareProperties.setProperty("url", "https://api.figshare.com/v2/oai");
        figshareProperties.setProperty("name", "figshare");
        figshareProperties.setProperty("metadata", "rdf");
        figshareProperties.setProperty("folder", testFolder.getRoot().toString());
        System.out.println("Test folder: " + testFolder.getRoot());
    }

    @org.junit.After
    public void tearDown() throws Exception {
        //nothing to do
    }

    @org.junit.Test
    public void testListSetsFromFigshare() throws Exception {
        //nothing to do
        Harvester harvester = new Harvester(figshareProperties);
        harvester.identify();
        Map<String, String> mapSets = harvester.listSets();

        System.out.println("Number of sets: " + mapSets.size());

        for (Map.Entry<String, String> entry : mapSets.entrySet()) {
                System.out.println(entry);
        }

        if (mapSets.size()==10) Assert.fail("Only 10 sets have been listed. Most likely, Harvester only loaded the first page, and the resume token does not work");

    }

}