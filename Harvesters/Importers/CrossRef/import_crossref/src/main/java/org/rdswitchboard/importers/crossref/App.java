package org.rdswitchboard.importers.crossref;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.rdswitchboard.libraries.neo4j.Neo4jDatabase;

public class App {
	private static final String PROPERTIES_FILE = "properties/import_crossref.properties";
	private static final String NEO4J_FOLDER = "neo4j";
	private static final String CROSSREF_FOLDER = "crossref/cahce";
	
	private static CrossrefGraph crossref;
	private static Neo4jDatabase neo4j;
	
	public static void main(String[] args) {
		try {
            String propertiesPath = PROPERTIES_FILE;
            if (args.length > 0 && StringUtils.isNotEmpty(args[0])) 
            	propertiesPath = args[0];

            Properties properties = new Properties();
            File propertiesFile = new File(propertiesPath);
            if (propertiesFile.exists() && propertiesFile.isFile()) {
		        try (InputStream in = new FileInputStream(propertiesFile)) {
		            properties.load(in);
		        }
            }
	        
	        String neo4jFolder = properties.getProperty("neo4j", NEO4J_FOLDER);
	        if (StringUtils.isNotEmpty(neo4jFolder))
	            throw new IllegalArgumentException("Neo4j Folder can not be empty");
	        System.out.println("Neo4J: " + neo4jFolder);
	     
	        String crossrefFolder = properties.getProperty("crossref", CROSSREF_FOLDER);
	        if (StringUtils.isNotEmpty(crossrefFolder))
	            throw new IllegalArgumentException("CrossRef Cache Folder can not be empty");
	        System.out.println("CrossRef: " + crossrefFolder);
	        
	        String sources = properties.getProperty("sources");
	        
	        crossref = new CrossrefGraph();
	        crossref.setCacheFolder(crossrefFolder);
	        
	        Neo4jDatabase neo4j = new Neo4jDatabase(neo4jFolder);
	        
	        if (null != sources) {
	        	String[] array = sources.split(",");
	        	for (String source : array) 
	        		
	        }
	     
		} catch (Exception e) {
            e.printStackTrace();
		}       
	}
	
	private static void process(String source) {
		String cypher = "MATCH (n";
		if (null != source)
			cypher += ":" + source;
		cypher += ""
	}
	
	/*
	
	private static final String NEO4J_URL = "http://localhost:7474/db/data/";	
//	private static final String NEO4J_URL = "http://localhost:7476/db/data/";	
//	private static final String NEO4J_URL = "http://ec2-54-187-84-58.us-west-2.compute.amazonaws.com:7474/db/data/";
	private static final String CROSSFRE_CACHE_FOLDER = "crossref/cahce";	
	
	public static void main(String[] args) {
		String neo4jUrl = NEO4J_URL;
		if (args.length > 0 && !args[0].isEmpty())
			neo4jUrl = args[0];
		
		String crossrefFolder = CROSSFRE_CACHE_FOLDER;
		if (args.length > 1 && !args[1].isEmpty())
			crossrefFolder = args[1];
			
		CrossrefGraph query = new CrossrefGraph();			
		
		
	}*/
}
