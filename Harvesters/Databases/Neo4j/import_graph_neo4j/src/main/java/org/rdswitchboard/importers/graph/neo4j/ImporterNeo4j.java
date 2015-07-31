package org.rdswitchboard.importers.graph.neo4j;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
import org.rdswitchboard.libraries.graph.GraphImporter;
import org.rdswitchboard.utils.neo4j.local.Neo4jUtils;

public class ImporterNeo4j implements GraphImporter {
	
	private Map<String, Index<Node>> indexes = new HashMap<String, Index<Node>>();
	private GraphDatabaseService graphDb;
	private boolean verbose = false;
	private long nodesCreated = 0;
	private long nodesUpdated = 0;
	private long relationshipsCreated = 0;
	private long relationshipsUpdated = 0;
	
	private final Map<String, List<GraphRelationship>> unknownRelationships = new HashMap<String, List<GraphRelationship>>();
	
	public ImporterNeo4j(final String neo4jFolder) throws Exception {
		
		graphDb = Neo4jUtils.getGraphDb( neo4jFolder );
	}
	
	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	public void resetCounters() {
		nodesCreated = nodesUpdated = relationshipsCreated = relationshipsUpdated = 0;
	}
	
	public void printStatistics(PrintStream out) {
		out.println( String.format("%d nodes has been created.\n%d nodes has been updated.\n%d relationships has been created.\n%d relationships has been updated.\n%d relationships keys has been invalid.", 
				nodesCreated, nodesUpdated, relationshipsCreated, relationshipsUpdated, unknownRelationships.size()) );
	}
	
	public void importGraph(Graph graph) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			_importSchemas(graph.getSchemas());
			_importNodes(graph.getNodes());
			_importRelationships(graph.getRelationships());
			
			tx.success();
		}
	}
	
	public void importSchemas(Collection<GraphSchema> schemas) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			_importSchemas(schemas);
		
			tx.success();
		}
	}

	public void importSchema(GraphSchema schema) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			_importSchema(schema);
		
			tx.success();
		}
	}
	
	public void importNodes(Collection<GraphNode> nodes) {
		// Import nodes
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			_importNodes(nodes);		
				
			tx.success();
		}
	}

	public void importNode(GraphNode node) {
		// Import nodes
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			_importNode(node);		
				
			tx.success();
		}
	}
	
	public void importRelationships(Collection<GraphRelationship> relationships) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{		
			_importRelationships(relationships);
			
			tx.success();
		}
	}
	
	public void importRelationship(GraphRelationship relationship) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{		
			_importRelationship(relationship, true);
			
			tx.success();
		}
	}
	
	private void _importSchemas(Collection<GraphSchema> schemas) {
		if (null != schemas)
			for (GraphSchema schema : schemas) 
				_importSchema(schema);
	}
	
	private void _importSchema(GraphSchema schema) {
		String label = schema.getLabel();
		String index = schema.getIndex();
		
		if (schema.isUnique()) {
			if (verbose) {
				System.out.println("Creating Constraint {label=" + label + ", index=" + index + "}");
			}
			Neo4jUtils.createConstrant(graphDb, label, index);
		} else {
			if (verbose) {
				System.out.println("Creating Index {label=" + label + ", index=" + index + "}");
			}

			Neo4jUtils.createIndex(graphDb, label, index);
		}
	}

	private void _importNodes(Collection<GraphNode> nodes) {
		// Import nodes
		if (null != nodes)
			for (GraphNode graphNode : nodes) 
				_importNode(graphNode);		
	}

	private void _importNode(GraphNode graphNode) {
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
			
			++nodesCreated;
		} else {
			if (null != graphNode.getProperties())
				for (Map.Entry<String, Object> entry : graphNode.getProperties().entrySet()) {  
					Object value = entry.getValue();
					if (null != value) 
						node.setProperty(entry.getKey(), value);
				}
			
			node.addLabel(DynamicLabel.label(source));
			node.addLabel(DynamicLabel.label(type));
			
			++nodesUpdated;
		}
		
		List<GraphRelationship> list = unknownRelationships.remove(getRelationshipKey(source, key));
		if (null != list) 
			for (GraphRelationship relationship : list) 
				_importRelationship(relationship, false);
	}
	
	private void _importRelationships(Collection<GraphRelationship> relationships) {
		if (null != relationships)
			for (GraphRelationship graphRelationship : relationships) 
				_importRelationship(graphRelationship, true);
	}
	
	private void _importRelationship(GraphRelationship graphRelationship, boolean storeUnknown) {
		GraphProperties start = graphRelationship.getStart();
		GraphProperties end = graphRelationship.getEnd();
			
		String relationshipName = graphRelationship.getRelationship();
		
		String startSource = (String) start.getProperty(GraphUtils.PROPERTY_SOURCE);
		Object startKey = start.getProperty(GraphUtils.PROPERTY_KEY);
		
		String endSource = (String) end.getProperty(GraphUtils.PROPERTY_SOURCE);
		Object endKey = end.getProperty(GraphUtils.PROPERTY_KEY);
				
		Node nodeStart = findNode(startSource, GraphUtils.PROPERTY_KEY, startKey);
		if (null == nodeStart && storeUnknown) 
			storeUnknownRelationship(getRelationshipKey(startSource, startKey), graphRelationship);
		Node nodeEnd = findNode(endSource, GraphUtils.PROPERTY_KEY, endKey);
		if (null == nodeEnd && storeUnknown)
			storeUnknownRelationship(getRelationshipKey(endSource, endKey), graphRelationship);
		
		if (null == nodeStart || null == nodeEnd)
			return;
		
		if (verbose) 
			System.out.println("Importing Relationship (source=" + startSource + ", key=" + startKey + ")-[" + relationshipName + "]->(source=" + endSource + ", key=" + endKey + ")");
		
		RelationshipType relationshipType = DynamicRelationshipType.withName(relationshipName);
		Relationship relationship = null;
		
		Iterable<Relationship> rels = nodeStart.getRelationships(relationshipType, Direction.OUTGOING);
		for (Relationship rel : rels) 
			if (rel.getEndNode().getId() == nodeEnd.getId()) {
				relationship = rel;
				break;
			}
				
		if (null == relationship) {
			relationship = nodeStart.createRelationshipTo(nodeEnd, relationshipType);
			++relationshipsCreated;
		} else if (null != graphRelationship.getProperties())
			++relationshipsUpdated;
		
		if (null != graphRelationship.getProperties())
			for (Map.Entry<String, Object> entry : graphRelationship.getProperties().entrySet())
				relationship.setProperty(entry.getKey(), entry.getValue());
	}
	
	private String getRelationshipKey(String source, Object key) {
		return source + "." + key.toString();
	}
	
	private void storeUnknownRelationship(String key, GraphRelationship relationship) {
		List<GraphRelationship> list = unknownRelationships.get(key);
		if (null == list) 
			unknownRelationships.put(key, list = new ArrayList<GraphRelationship>());
		
		list.add(relationship);
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
