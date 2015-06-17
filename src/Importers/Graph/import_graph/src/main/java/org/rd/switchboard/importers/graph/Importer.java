package org.rd.switchboard.importers.graph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.rd.switchboard.utils.graph.GraphNode;
import org.rd.switchboard.utils.graph.GraphRelationship;
import org.rd.switchboard.utils.neo4j.Neo4jUtils;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.batch.BatchCallback;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Importer {
	private static final String FOLDER_NODE = "node";
	private static final String FOLDER_RELATIONSHIP = "relationship";
	private static final int MAX_COMMANDS = 1024;
	
	private File outputFolder;
	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	private ObjectMapper mapper; 
	
	private Set<String> constrants = new HashSet<String>();
//	private Set<String> indexes = new HashSet<String>();
	
//	private Map<String, RestIndex<Node>> mapIndexes = new HashMap<String, RestIndex<Node>>();
	
	/*
	private RestIndex<Node> getIndex(Label labelSource, Label labelType) {
		String label = Neo4jUtils.combineLabel(labelSource, labelType);
		RestIndex<Node> index = mapIndexes.get(label);
		if (null == index) 
			mapIndexes.put(label, index = Neo4jUtils.getIndex(graphDb, label));
				
		return index;
	}
	
	private RestIndex<Node> getIndex(Label labelSource) {
		String label = labelSource.name();
		RestIndex<Node> index = mapIndexes.get(label);
		if (null == index) 
			mapIndexes.put(label, index = Neo4jUtils.getIndex(graphDb, label));
				
		return index;
	}*/
	
	public Importer(final String neo4jUrl, final String outputFolder) {
		System.out.println("Source Neo4j: " + neo4jUrl);
		System.out.println("Target folder: " + outputFolder);
	
		// setup Object mapper
		mapper = new ObjectMapper(); 
		
		// connect to graph database
		graphDb = new RestAPIFacade(neo4jUrl);  
				
		// Create cypher engine
		engine = new RestCypherQueryEngine(graphDb);  
		
		// Set output folder
		this.outputFolder = new File(outputFolder);
		
		// Config neo4j to use Bacth transactions
	//	System.setProperty("org.neo4j.rest.batch_transaction", "true");
		
	}

	public void process() throws IOException {
		importNodes();
		importRelationships();
	}
	
	private void importNodes() throws IOException {
	//	Transaction tx = null;
		
		File folder = new File (outputFolder, FOLDER_NODE);
		if (folder.isDirectory()) {
			File[] nodes = folder.listFiles();

			List<GraphNode> graphNodes = null;
			
			long beginTime = System.currentTimeMillis();
			int counter = 0;
			int counterTotal = 0;
			
			for (File nodeFile : nodes) 
				if (!nodeFile.isDirectory()) { 
				//	System.out.println("Processing node: " + nodeFile.getName());
					
					GraphNode node = mapper.readValue(nodeFile, GraphNode.class);
					
					String source = (String) node.getProperties().get(Neo4jUtils.PROPERTY_NODE_SOURCE);
					if (null == source || source.isEmpty()) {
						System.out.println("Error in node: " + nodeFile.toString() + ", the node source is empty");
						continue;
					}
					
					createConstrant(source); 
	
					if (null == graphNodes)
						graphNodes = new ArrayList<GraphNode>();
					
					graphNodes.add(node);
					
					if (++counter >= MAX_COMMANDS) {
						mergeNodes(graphNodes);
						graphNodes = null;
						
						counterTotal += counter;
						counter = 0;
					}
				}
			
			if (null != graphNodes) {
				
				mergeNodes(graphNodes);
				
				counterTotal += counter;
			}		
			
			long endTime = System.currentTimeMillis();
			
			System.out.println(String.format("Done. Imporded %d nodes over %d ms. Average %f ms per node", 
					counterTotal, endTime - beginTime, (float)(endTime - beginTime) / (float)counterTotal));
		}
		else
			System.out.println("Error. The path " + folder.getAbsolutePath() + " is not exist");
	}
	
	private void importRelationships() throws IOException {
		File folder = new File (outputFolder, FOLDER_RELATIONSHIP);
		if (folder.isDirectory()) {
			File[] relationships = folder.listFiles();

			List<GraphRelationship> graphRelationships = null;
			
			long beginTime = System.currentTimeMillis();
			int counter = 0;
			int counterTotal = 0;
			
			for (File relationshipFile : relationships) 
				if (!relationshipFile.isDirectory()) { 
				//	System.out.println("Processing relationship: " + relationshipFile.getName());
					
					GraphRelationship relationship = mapper.readValue(relationshipFile, GraphRelationship.class);
			
					if (null == graphRelationships)
						graphRelationships = new ArrayList<GraphRelationship>();
					
					graphRelationships.add(relationship);
					
					if (++counter >= MAX_COMMANDS) {
						createUniqueRelationships(graphRelationships);
						graphRelationships = null;
						
						counterTotal += counter;
						counter = 0;
					}
				}
			
			if (null != graphRelationships) {
				
				createUniqueRelationships(graphRelationships);
				
				counterTotal += counter;
			}		
			
			long endTime = System.currentTimeMillis();
			
			System.out.println(String.format("Done. Imporded %d relationships over %d ms. Average %f ms per relationship", 
					counterTotal, endTime - beginTime, (float)(endTime - beginTime) / (float)counterTotal));
		}
		else
			System.out.println("Error. The path " + folder.getAbsolutePath() + " is not exist");
	}
	
	private void mergeNodes(final List<GraphNode> nodes) {
		
		graphDb.executeBatch(new BatchCallback<Void>() {
			@Override
			public Void recordBatch(RestAPI batchRestApi) {
				//RestCypherQueryEngine engine = new RestCypherQueryEngine(batchRestApi);
				
				for (GraphNode node : nodes) {
					String source = (String) node.getProperties().get(Neo4jUtils.PROPERTY_NODE_SOURCE);
					if (null == source || source.isEmpty()) {
						continue;
					}
					String type = (String) node.getProperties().get(Neo4jUtils.PROPERTY_NODE_TYPE);
					if (null == type || type.isEmpty()) {
						continue;
					}

					Neo4jUtils.mergeNode(batchRestApi, type, source, node.getProperties());
				}
				
				return null;
			}
		});
	}
	
	private void createUniqueRelationships(final List<GraphRelationship> relationships) {
		graphDb.executeBatch(new BatchCallback<Void>() {
			@Override
			public Void recordBatch(RestAPI batchRestApi) {
				//RestCypherQueryEngine engine = new RestCypherQueryEngine(batchRestApi);
				
				for (GraphRelationship rel : relationships) {
					
					Neo4jUtils.createUniqueRelationship(batchRestApi, 
							rel.getStart().getSource(), // + ":" + rel.getStart().getType(),
							Neo4jUtils.PROPERTY_KEY, rel.getStart().getKey(),
							rel.getEnd().getSource(), // + ":" + rel.getEnd().getType(),
							Neo4jUtils.PROPERTY_KEY, rel.getEnd().getKey(),
							rel.getRelationship(), rel.getProperties());
				}
				
				return null;
			}
		});
	}
	

	private void createConstrant(String label) {
		if (!constrants.contains(label)) {
			Neo4jUtils.createConstraint(engine, label, Neo4jUtils.PROPERTY_KEY);
			constrants.add(label);
		}
	}
	
	/*
	private void createIndex(String label) {
		if (!indexes.contains(label)) {
			Neo4jUtils.createIndex(engine, label, Neo4jUtils.PROPERTY_KEY);
			indexes.add(label);
		}
	}*/
	
	
}
