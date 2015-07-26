package org.rdswitchboard.importers.patterns;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.parboiled.common.StringUtils;
import org.rdswitchboard.importers.graph.neo4j.Importer;
import org.rdswitchboard.libraries.graph.Graph;
import org.rdswitchboard.libraries.graph.GraphNode;
import org.rdswitchboard.libraries.graph.GraphRelationship;
import org.rdswitchboard.libraries.graph.GraphSchema;
import org.rdswitchboard.libraries.graph.GraphUtils;

import au.com.bytecode.opencsv.CSVReader;

public class App {
	private static final String PROPERTIES_FILE = "properties/import_patterns.properties";
	private static final String PATTERNS_SCV_FILE = "data/patterns.csv";
	
	/**
	 * Main class function
	 * @param args String[] Neo4J URL.
	 * If missing, the default parameters will be used.
	 */
	public static void main(String[] args) {
		try {
			String propertiesFile = PROPERTIES_FILE;
	        if (args.length > 0 && !StringUtils.isEmpty(args[0])) 
	        	propertiesFile = args[0];
	
	        Properties properties = new Properties();
	        try (InputStream in = new FileInputStream(propertiesFile)) {
	            properties.load(in);
	        }
	        
	        System.out.println("Importing Web Patterns");
	                
	        String neo4jFolder = properties.getProperty("neo4j");
	        if (StringUtils.isEmpty(neo4jFolder))
	            throw new IllegalArgumentException("Neo4j Folder can not be empty");
	        System.out.println("Neo4j: " + neo4jFolder);
	        
	        String patterns = properties.getProperty("patterns", PATTERNS_SCV_FILE);
	        if (StringUtils.isEmpty(patterns))
	            throw new IllegalArgumentException("Invalid path to Patterns CSV file");
	        
	        List<GraphSchema> schemas = new ArrayList<GraphSchema>();
	        schemas.add( new GraphSchema()
	        		.withLabel(GraphUtils.SOURCE_WEB)
	        		.withIndex(GraphUtils.PROPERTY_KEY)
	        		.withUnique(true));
	        
			Importer importer = new Importer(neo4jFolder);
			importer.setVerbose(true);
			importer.ImportSchemas(schemas);
			
			Graph graph = importPatternsCsv(patterns);
			if (null == graph)
				return;
			
			importer.ImportNodes(graph.getNodes());
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	private static Graph importPatternsCsv(final String csv) {
		// Imoprt Grant data
		System.out.println("Importing file: " + csv);
					
		// process grats data file
		CSVReader reader;
		Graph graph = new Graph();
		try 
		{
			reader = new CSVReader(new FileReader(csv));
			String[] pattern;
			boolean header = false;
			while ((pattern = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (pattern.length != 2)
					continue;
						
				String url = pattern[0];
				String pat = pattern[1];
				
				System.out.println("url: " + pat);
				
				if (null != url && !url.isEmpty()) {
					
					URL hostUrl = null;
					
					try {
						hostUrl = new URL(url);
					} catch(MalformedURLException ex) {
						hostUrl = new URL("http://" + url);
					}
					
					String host = hostUrl.getHost();
					if (host.startsWith("www."))
						host = host.substring(4);
					else if (host.startsWith("www3."))
						host = host.substring(5);
					else if (host.startsWith("web."))
						host = host.substring(4);
					
					graph.addNode(new GraphNode()
						.withKey(host)
						.withSource(GraphUtils.SOURCE_WEB)
						.withType(GraphUtils.TYPE_INSTITUTION)
						.withProperty(GraphUtils.PROPERTY_PATTERN, pat)
						.withProperty(GraphUtils.PROPERTY_HOST, host));
				}
			}
				
			reader.close();			
		} catch (Exception e) {
			e.printStackTrace();
			
			return null;
		} 
				
		return graph;
	}
}
