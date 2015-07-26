package org.rdswitchboard.importers.graph.neo4j;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.rdswitchboard.libraries.graph.Graph;
import org.rdswitchboard.libraries.graph.GraphNode;
import org.rdswitchboard.libraries.graph.GraphProperties;
import org.rdswitchboard.libraries.graph.GraphRelationship;
import org.rdswitchboard.libraries.graph.GraphSchema;
import org.rdswitchboard.libraries.graph.GraphUtils;
import org.rdswitchboard.utils.neo4j.local.Neo4jUtils;

public class Importer {
	
	private Map<String, Index<Node>> indexes = new HashMap<String, Index<Node>>();
	private GraphDatabaseService graphDb;
	private boolean verbose = false;
	
	public Importer(final String neo4jFolder) throws Exception {
		
		graphDb = Neo4jUtils.getGraphDb( neo4jFolder );
	}
	
	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	public void ImportSchemas(Collection<GraphSchema> schemas) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			for (GraphSchema schema : schemas) 
				if (schema.isUnique())
					Neo4jUtils.createConstrant(graphDb, schema.getLabel(), schema.getIndex());
				else
					Neo4jUtils.createIndex(graphDb, schema.getLabel(), schema.getIndex());
		
			tx.success();
		}
	}

	public void ImportNodes(Collection<GraphNode> nodes) {
		// Import nodes
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			if (null != nodes)
				for (GraphNode graphNode : nodes) 
					importNode(graphNode);		
				
			tx.success();
		}
	}

	public void ImportRelationships(Collection<GraphRelationship> relationships) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{		
			if (null != relationships)
				for (GraphRelationship graphRelationship : relationships) 
					importRelationship(graphRelationship);
			
			tx.success();
		}
	}

	private void importNode(GraphNode graphNode) {
		String source = (String) graphNode.getSource();
		String type = (String) graphNode.getType();
		String key = (String) graphNode.getKey();
		
		if (verbose) {
			System.out.println("Importing Node (source=" + source + ", type=" + type + ", key=" + key + ")");
		}
		
		Index<Node> index = getIndex(source);
		Node node = (Node) index.get(GraphUtils.PROPERTY_KEY, key).getSingle();
		if (null == node) {
			node = graphDb.createNode();
				 	
			if (null != graphNode.getProperties())
				for (Map.Entry<String, Object> entry : graphNode.getProperties().entrySet()) {  
					Object value = entry.getValue();
					if (null != value) 
						node.setProperty(entry.getKey(), value);
				}
			
			node.addLabel(DynamicLabel.label(source));
			node.addLabel(DynamicLabel.label(type));
			
			index.add(node, GraphUtils.PROPERTY_KEY, key);
		} else {
			if (verbose) {
				System.out.println("Node exists");
			}
			
			if (null != graphNode.getProperties())
				for (Map.Entry<String, Object> entry : graphNode.getProperties().entrySet()) {  
					Object value = entry.getValue();
					if (null != value) 
						node.setProperty(entry.getKey(), value);
				}
			
			node.addLabel(DynamicLabel.label(source));
			node.addLabel(DynamicLabel.label(type));
		}
	}
	
	private void importRelationship(GraphRelationship graphRelationship) {
		GraphProperties start = graphRelationship.getStart();
		GraphProperties end = graphRelationship.getEnd();
			
		String relationshipName = graphRelationship.getRelationship();
		
		String startSource = (String) start.getProperty(GraphUtils.PROPERTY_SOURCE);
		Object startKey = start.getProperty(GraphUtils.PROPERTY_KEY);
		
		String endSource = (String) end.getProperty(GraphUtils.PROPERTY_SOURCE);
		Object endKey = end.getProperty(GraphUtils.PROPERTY_KEY);
		
		if (verbose) {
			System.out.println("Importing Relationship (source=" + startSource + ", key=" + startKey + ")-[" + relationshipName + "]->(source=" + endSource + ", key=" + endKey + ")");
		}
		
		Node nodeStart = findNode(startSource, GraphUtils.PROPERTY_KEY, startKey);
		if (null == nodeStart) { 
			System.out.println("Error in graph, unable to find start node " + startSource + " with key: " + startKey);
			return;
		}
		Node nodeEnd = findNode(endSource, GraphUtils.PROPERTY_KEY, endKey);
		if (null == nodeEnd) {
			System.out.println("Error in graph, unable to find end node " + endSource + " with key: " + endKey);
			return;
		}

		RelationshipType relationship = DynamicRelationshipType.withName(relationshipName);
		
		Iterable<Relationship> rels = nodeStart.getRelationships(relationship, Direction.OUTGOING);
		for (Relationship rel : rels) 
			if (rel.getEndNode().getId() == nodeEnd.getId())
				return;
		
		Relationship rel = nodeStart.createRelationshipTo(nodeEnd, relationship);
			
		if (null != graphRelationship.getProperties())
			for (Map.Entry<String, Object> entry : graphRelationship.getProperties().entrySet())
				rel.setProperty(entry.getKey(), entry.getValue());
	}
	
	private Index<Node> getIndex(String label) {
		if ( indexes.containsKey(label) ) 
			return indexes.get( label );
		
		Index<Node> index = graphDb.index().forNodes( label );
		indexes.put(label, index);
		return index;
	}
	
	private Node findNode(String label, String key, Object value) {
		return getIndex(label)
				.get(key, value)
				.getSingle();
		
	/*	ResourceIterable<Node> nodes = graphDb.findNodesByLabelAndProperty(
				DynamicLabel.label(connection.getType()), 
				AggrigationUtils.PROPERTY_KEY, 
				connection.getKey());
		
		try (ResourceIterator<Node> noded = nodes.iterator()) {
			if (noded.hasNext())
				return noded.next();
			else
				return null;
		}*/
	}
}
