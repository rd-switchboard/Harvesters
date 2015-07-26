package org.rdswitchboard.importers.institutions;

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
import org.rdswitchboard.libraries.graph.GraphSchema;
import org.rdswitchboard.libraries.graph.GraphUtils;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Main class for Web:Institution importer
 * 
 * This software design to process institutions.csv file located in the working directory
 * and will post data into Neo4J located at http://localhost:7474/db/data/
 * <p>
 * The institutions.csv fomat should be:
 * <br>
 * country,state,institution_name,institution_url
 * <p>
 * The first file line will be counted as header and will be ignored. The Institution host will be 
 * automatically extracted from an institution url
 * 
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.0.0
 */


public class App {
	private static final String PROPERTIES_FILE = "properties/import_institutions.properties";
	private static final String INSTITUTIONS_SCV_FILE = "data/institutions.csv";
	
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
	        
	        System.out.println("Importing Web Institutions");
	                
	        String neo4jFolder = properties.getProperty("neo4j");
	        if (StringUtils.isEmpty(neo4jFolder))
	            throw new IllegalArgumentException("Neo4j Folder can not be empty");
	        System.out.println("Neo4j: " + neo4jFolder);
	        
	        String institutions = properties.getProperty("institutions", INSTITUTIONS_SCV_FILE);
	        if (StringUtils.isEmpty(institutions))
	            throw new IllegalArgumentException("Invalid path to Institutions CSV file");
	        
	        List<GraphSchema> schemas = new ArrayList<GraphSchema>();
	        schemas.add( new GraphSchema()
	        		.withLabel(GraphUtils.SOURCE_WEB)
	        		.withIndex(GraphUtils.PROPERTY_KEY)
	        		.withUnique(true));
	        
			Importer importer = new Importer(neo4jFolder);
			importer.setVerbose(true);
			importer.ImportSchemas(schemas);
			
			Graph graph = importInstitutionsCsv(institutions);
			if (null == graph)
				return;
			
			importer.ImportNodes(graph.getNodes());
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	private static Graph importInstitutionsCsv(final String csv) {
		// Imoprt Grant data
		System.out.println("Importing file: " + csv);
					
		// process grats data file
		CSVReader reader;
		Graph graph = new Graph();
		try 
		{
			reader = new CSVReader(new FileReader(csv));
			String[] institution;
			boolean header = false;
			while ((institution = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (institution.length != 4)
					continue;
						
				String country = institution[0];
				String state = institution[1];
				String title = institution[2];
				String url = institution[3];
				
				System.out.println("Institution: " + url);
				
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
					
				GraphNode node = new GraphNode()
					.withKey(host)
					.withSource(GraphUtils.SOURCE_WEB)
					.withType(GraphUtils.TYPE_INSTITUTION)
					.withProperty(GraphUtils.PROPERTY_TITLE, title)
					.withProperty(GraphUtils.PROPERTY_URL, url)
					.withProperty(GraphUtils.PROPERTY_HOST, host);
						
				if (StringUtils.isNotEmpty(country))
					node.setProperty(GraphUtils.PROPERTY_COUNTRY, country);
				if (StringUtils.isNotEmpty(state))
					node.setProperty(GraphUtils.PROPERTY_STATE, state);
				
				graph.addNode(node);
			}
				
			reader.close();			
		} catch (Exception e) {
			e.printStackTrace();
			
			return null;
		} 
				
		return graph;
	}
}
