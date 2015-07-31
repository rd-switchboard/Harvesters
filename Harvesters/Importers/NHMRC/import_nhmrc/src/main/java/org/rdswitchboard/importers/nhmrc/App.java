package org.rdswitchboard.importers.nhmrc;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.parboiled.common.StringUtils;
import org.rdswitchboard.importers.graph.neo4j.ImporterNeo4j;
import org.rdswitchboard.libraries.graph.Graph;
import org.rdswitchboard.libraries.graph.GraphNode;
import org.rdswitchboard.libraries.graph.GraphRelationship;
import org.rdswitchboard.libraries.graph.GraphSchema;
import org.rdswitchboard.libraries.graph.GraphUtils;

import au.com.bytecode.opencsv.CSVReader;

/**
 * HISTORY:
 * 1.0.1: Switched import to server neo4j-1.1
 * 
 * @author dima
 *
 */

public class App {
	private static final String PROPERTIES_FILE = "properties/import_nhmrc.properties";
	
	private static final String GRANTS_CSV_PATH = "data/nhmrc/2014/grants-data.csv";
	private static final String ROLES_CSV_PATH = "data/nhmrc/2014/ci-roles.csv";

			
	private static final Set<String> institutions = new HashSet<String>();
	private static final Set<String> researcher = new HashSet<String>();
	
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
	        
	        System.out.println("Importing NHMRC Grants");
	                
	        String neo4jFolder = properties.getProperty("neo4j");
	        if (StringUtils.isEmpty(neo4jFolder))
	            throw new IllegalArgumentException("Neo4j Folder can not be empty");
	        System.out.println("Neo4j: " + neo4jFolder);
	        
	        String grantsFile = properties.getProperty("grants", GRANTS_CSV_PATH);
	        if (StringUtils.isEmpty(grantsFile))
	            throw new IllegalArgumentException("Invalid path to the Grants CSV file");
	        
	        String rolesFile = properties.getProperty("new.grants", ROLES_CSV_PATH);
	        if (StringUtils.isEmpty(rolesFile))
	            throw new IllegalArgumentException("Invalid path to the Roles CSV file");
	        
	        List<GraphSchema> schema = new ArrayList<GraphSchema>();
	        schema.add( new GraphSchema()
	        		.withLabel(GraphUtils.SOURCE_NHMRC)
	        		.withIndex(GraphUtils.PROPERTY_KEY)
	        		.withUnique(true));
	        
			ImporterNeo4j importer = new ImporterNeo4j(neo4jFolder);
			importer.setVerbose(true);
			importer.importSchemas(schema);
			
			Graph graph = importGrantsCsv(grantsFile);
			if (null == graph)
				return;
			
			importer.importNodes(graph.getNodes());
			importer.importRelationships(graph.getRelationships());
			
			graph = importRolesCsv(rolesFile);
			if (null == graph)
				return;
			
			importer.importNodes(graph.getNodes());
			importer.importRelationships(graph.getRelationships());			
			
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	private static Graph importGrantsCsv(final String csv) {
		// Imoprt Grant data
		System.out.println("Importing file: " + csv);
					
		// process grats data file
		CSVReader reader;
		Graph graph = new Graph();
		try 
		{
			reader = new CSVReader(new FileReader(csv));
			String[] grant;
			boolean header = false;
			while ((grant = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (grant.length != 57)
					continue;
						
				String grantId = grant[0];
				System.out.println("Grant id: " + grantId);
				
				String purl = "http://purl.org/au-research/grants/nhmrc/" + grantId;
				String institutionName = grant[6].trim();
				String institutionKey = "nhmrc:institution:" + institutionName;
				String title = grant[9]; // grant[10] - simplified							
				String investigatorString = grant[6];
									
				if (!institutions.contains(institutionKey)) {
					institutions.add(institutionKey);

					graph.addNode(new GraphNode()
						.withKey(institutionKey)
						.withSource(GraphUtils.SOURCE_NHMRC)
						.withType(GraphUtils.TYPE_INSTITUTION)
						.withProperty(GraphUtils.PROPERTY_TITLE, institutionName));
					
				}					
					
				graph.addNode(new GraphNode()
					.withKey(purl)
					.withSource(GraphUtils.SOURCE_NHMRC)
					.withType(GraphUtils.TYPE_GRANT)
					.withProperty(GraphUtils.PROPERTY_URL, purl)
					.withProperty(GraphUtils.PROPERTY_PURL, purl)
					.withProperty(GraphUtils.PROPERTY_LOCAL_ID, grantId)
					.withProperty(GraphUtils.PROPERTY_TITLE, title));
				
				graph.addRelationship(new GraphRelationship()
					.withRelationship("AdminInstitute")
					.withStartSource(GraphUtils.SOURCE_NHMRC)
					.withStartKey(purl)
					.withEndSource(GraphUtils.SOURCE_NHMRC)
					.withEndKey(institutionKey));
			}
				
			reader.close();			
		} catch (Exception e) {
			e.printStackTrace();
			
			return null;
		} 
				
		return graph;
	}
	
	private static Graph importRolesCsv(final String csv) {
		// Imoprt Grant data
		System.out.println("Importing file: " + csv);
					
		// process grats data file
		CSVReader reader;
		Graph graph = new Graph();
		try 
		{
			reader = new CSVReader(new FileReader(csv));
			String[] grantee;
			boolean header = false;
			while ((grantee = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (grantee.length != 12)
					continue;
						
				String grantId = grantee[0];
				String individualId = grantee[2];
				System.out.println("Grant id: " + grantId + ", Individual ID: " + individualId);
				
				String purl = "http://purl.org/au-research/grants/nhmrc/" + grantId;
				String key = "nhmrc:researcher:" + individualId;
				
				if (!researcher.contains(individualId)) {
					researcher.add(individualId);
					
					String prefix = grantee[4].trim();
					String firstName = grantee[5].trim();
					String middleName = grantee[6].trim();
					String lastName = grantee[7].trim();
					String fullName = grantee[8].trim();
									
					GraphNode node = new GraphNode()
						.withKey(key)
						.withSource(GraphUtils.SOURCE_NHMRC)
						.withType(GraphUtils.TYPE_RESEARCHER)
						.withProperty(GraphUtils.PROPERTY_LOCAL_ID, individualId)
						.withProperty(GraphUtils.PROPERTY_TITLE, fullName);
					if (StringUtils.isNotEmpty(prefix))
						node.setProperty(GraphUtils.PROPERTY_NAME_PREFIX, prefix);
					if (StringUtils.isNotEmpty(firstName))
						node.setProperty(GraphUtils.PROPERTY_FIRST_NAME, firstName);
					if (StringUtils.isNotEmpty(middleName))
						node.setProperty(GraphUtils.PROPERTY_MIDDLE_NAME, middleName);
					if (StringUtils.isNotEmpty(lastName))
						node.setProperty(GraphUtils.PROPERTY_LAST_NAME, lastName);
					if (StringUtils.isNotEmpty(fullName))
						node.setProperty(GraphUtils.PROPERTY_FULL_NAME, fullName);
					
					graph.addNode(node);
				}
				
				graph.addRelationship(new GraphRelationship()
					.withRelationship("Investigator")
					.withStartSource(GraphUtils.SOURCE_NHMRC)
					.withStartKey(key)
					.withEndSource(GraphUtils.SOURCE_NHMRC)
					.withEndKey(purl));
			}
				
			reader.close();			
		} catch (Exception e) {
			e.printStackTrace();
			
			return null;
		} 
				
		return graph;
	}
}