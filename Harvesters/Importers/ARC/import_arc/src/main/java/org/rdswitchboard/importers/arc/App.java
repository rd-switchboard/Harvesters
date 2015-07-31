package org.rdswitchboard.importers.arc;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.parboiled.common.StringUtils;
import org.rdswitchboard.importers.graph.neo4j.ImporterNeo4j;
import org.rdswitchboard.libraries.graph.Graph;
import org.rdswitchboard.libraries.graph.GraphImporter;
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
	private static final String PROPERTIES_FILE = "properties/import_arc.properties";
	
	private static final String COMPLETED_GRANTS_CSV_PATH = "data/arc/completed_projects.csv";
	//private static final String COMPLETED_ROLES_CSV_PATH = "data/arc/completed_fellowships.csv";
	private static final String NEW_GRANTS_CSV_PATH = "data/arc/new_projects.csv";
	//private static final String NEW_ROLES_CSV_PATH = "data/arc/new_fellowships.csv";
	
	private static final String[] TITLES = { "Mr ", "Ms ", "Dr ", "A/Prof ", "Adj/Prof ", "Asst Prof ", "Prof Dr ", "Prof " }; 
			
	private static final Set<String> institutions = new HashSet<String>();
	private static final Set<String> grants = new HashSet<String>();
	
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
	        
	        System.out.println("Importing ARC Grants");
	                
	        String neo4jFolder = properties.getProperty("neo4j");
	        if (StringUtils.isEmpty(neo4jFolder))
	            throw new IllegalArgumentException("Neo4j Folder can not be empty");
	        System.out.println("Neo4j: " + neo4jFolder);
	        
	        String completedGrants = properties.getProperty("completed.grants", COMPLETED_GRANTS_CSV_PATH);
	        if (StringUtils.isEmpty(completedGrants))
	            throw new IllegalArgumentException("Invalid path to completed grants CSV file");
	        
	        String newGrants = properties.getProperty("new.grants", NEW_GRANTS_CSV_PATH);
	        if (StringUtils.isEmpty(newGrants))
	            throw new IllegalArgumentException("Invalid path to new grants CSV file");
	        
	        List<GraphSchema> schema = new ArrayList<GraphSchema>();
	        schema.add( new GraphSchema()
	        		.withLabel(GraphUtils.SOURCE_ARC)
	        		.withIndex(GraphUtils.PROPERTY_KEY)
	        		.withUnique(true));
	        
			GraphImporter importer = new ImporterNeo4j(neo4jFolder);
			importer.setVerbose(true);
			importer.importSchemas(schema);
			
			Graph graph = importGrantsCsv(completedGrants);
			if (null == graph)
				return;
			
			importer.importNodes(graph.getNodes());
			importer.importRelationships(graph.getRelationships());
			
			graph = importGrantsCsv(newGrants);
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
				if (grant.length != 29)
					continue;
						
				String projectId = grant[0];
				System.out.println("Project id: " + projectId);
				
				if (!grants.contains(projectId)) {
					grants.add(projectId);
					
					String purl = "http://purl.org/au-research/grants/arc/" + projectId;
					String institutionName = grant[4];
					String institutionKey = "arc:institution:"+ institutionName;
					String title = grant[7];										
					String investigatorString = grant[6];
										
					if (!institutions.contains(institutionKey)) {
						institutions.add(institutionKey);

						graph.addNode(new GraphNode()
							.withKey(institutionKey)
							.withSource(GraphUtils.SOURCE_ARC)
							.withType(GraphUtils.TYPE_INSTITUTION)
							.withProperty(GraphUtils.PROPERTY_TITLE, institutionName));
						
					}					
						
					graph.addNode(new GraphNode()
						.withKey(purl)
						.withSource(GraphUtils.SOURCE_ARC)
						.withType(GraphUtils.TYPE_GRANT)
						.withProperty(GraphUtils.PROPERTY_URL, purl)
						.withProperty(GraphUtils.PROPERTY_PURL, purl)
						.withProperty(GraphUtils.PROPERTY_LOCAL_ID, projectId)
						.withProperty(GraphUtils.PROPERTY_TITLE, title));
					
					graph.addRelationship(new GraphRelationship()
						.withRelationship("AdminInstitute")
						.withStartSource(GraphUtils.SOURCE_ARC)
						.withStartKey(purl)
						.withEndSource(GraphUtils.SOURCE_ARC)
						.withEndKey(institutionKey));
							
					if (!investigatorString.contains("n.a.")) {
									
						Set<String> investigators = null;
						if (investigatorString.contains(";"))
							investigators = new HashSet<String>(Arrays.asList(investigatorString.split(";")));
						else
						{
							investigators = new HashSet<String>();
									
							int tagSize = 0;
							while (null != investigatorString && !investigatorString.isEmpty()) {
								int pos = -1;
								int size = 0;
								for (String ttl : TITLES) {
									int pos1 = investigatorString.indexOf(ttl, tagSize);
									if (pos1 != -1 && (pos == -1 || pos > pos1)) {
										pos = pos1;
										size = ttl.length();
									}
								}
								if (pos != -1) {
									// we have find a tag
									tagSize = size;
									
									if (pos > 0) {									
										String inv = investigatorString.substring(0, pos).trim();
										investigatorString = investigatorString.substring(pos);
										if (!inv.isEmpty()) {
										//	System.out.println(inv);
											investigators.add(inv);
										}
									}
								} else {
									// we didn't find any 
									
									investigatorString = investigatorString.trim();
									if (!investigatorString.isEmpty()) {
								//		System.out.println(investigator);
										investigators.add(investigatorString);
									}
									investigatorString = null;
								}
							}
						}
								
						if (null != investigators && !investigators.isEmpty()) {
							for (String grantee : investigators) {
								String granteeName = grantee.trim();
								String granteeKey = "arc:researcher:" + projectId + ":" + granteeName;
								
								graph.addNode(new GraphNode()
									.withKey(granteeKey)
									.withSource(GraphUtils.SOURCE_ARC)
									.withType(GraphUtils.TYPE_RESEARCHER)
									.withProperty(GraphUtils.PROPERTY_TITLE, granteeName));
																
								graph.addRelationship(new GraphRelationship()
									.withRelationship("Investigator")
									.withStartSource(GraphUtils.SOURCE_ARC)
									.withStartKey(granteeKey)
									.withEndSource(GraphUtils.SOURCE_ARC)
									.withEndKey(purl));
							}
						}
					}
				}
				else
					System.out.println("The Grants map already contains the key: " + projectId);
			}
				
			reader.close();			
		} catch (Exception e) {
			e.printStackTrace();
			
			return null;
		} 
				
		return graph;
	}
}
