package org.rdswitchboard.importers.crossref;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.rdswitchboard.libraries.graph.Graph;
import org.rdswitchboard.libraries.graph.GraphNode;
import org.rdswitchboard.libraries.graph.GraphRelationship;
import org.rdswitchboard.libraries.graph.GraphUtils;
import org.rdswitchboard.libraries.neo4j.Neo4jDatabase;
import org.rdswitchboard.libraries.neo4j.interfaces.ProcessNode;

public class App {
	private static final String PROPERTIES_FILE = "properties/import_crossref.properties";
	private static final String NEO4J_FOLDER = "neo4j";
	private static final String CROSSREF_FOLDER = "crossref/cahce";
	
	private static final String PART_DOI = "doi:";
	
	private static CrossrefGraph crossref;
	private static Neo4jDatabase neo4j;
	
	private static Map<String, String> dois = new HashMap<String, String>();
	
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
	        
	     /*   String sources = properties.getProperty("sources");
	        if (StringUtils.isNotEmpty(sources))
	            throw new IllegalArgumentException("Sources can not be empty");
	        System.out.println("Sources: " + crossrefFolder);*/
	        
	        crossref = new CrossrefGraph();
	        crossref.setCacheFolder(crossrefFolder);
	        
	        neo4j = new Neo4jDatabase(neo4jFolder);
	        neo4j.setVerbose(true);
	        
	   /*     if (null != sources) {
	        	String[] array = sources.split(",");
	        	for (String source : array) 
	        		process(source);
	        }*/
	        
	        process(GraphUtils.SOURCE_DRYAD, GraphUtils.PROPERTY_REFERENCED_BY);
	     
		} catch (Exception e) {
            e.printStackTrace();
		}       
	}
	
	private static void process(final String source, final String property) {
		System.out.println("Processing source: " + source + ", property: " + property);
		
		final Graph graph = new Graph();
		
		neo4j.createIndex(DynamicLabel.label(source), property);
		neo4j.enumrateAllNodesWithLabelAndProperty(source, property, new ProcessNode() {

			@Override
			public void processNode(Node node) {
				String key = (String) node.getProperty(GraphUtils.PROPERTY_KEY);
				Object dois = node.getProperty(property);
				if (dois instanceof String) {
					processDoi(graph, source, key, (String)dois); 
				} else if (dois instanceof String[]) {
					for (String doi : (String[])dois)
						processDoi(graph, source, key, doi);
				}
			}			
		});
		
		System.out.println("Importing nodes");
		
		neo4j.importGraph(graph);
	}
	
	private static void processDoi(Graph graph, String source, String nodeKey, String doi) {
		String crKey = null;
		if (dois.containsKey(doi)) {
			crKey = dois.get(doi);
		} else {
			GraphNode node = crossref.queryGraph(graph, PART_DOI + doi);
			dois.put(doi, crKey = (String) node.getKey().getValue());
		}
			
		if (null != crKey) 
			graph.addRelationship(new GraphRelationship() 
					.withRelationship(GraphUtils.RELATIONSHIP_KNOWN_AS)
					.withStart(source, nodeKey)
					.withEnd(GraphUtils.SOURCE_CROSSREF, crKey));
	}
}
