package org.rdswitchboard.utils.neo4j.copy.harmonized;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.tooling.GlobalGraphOperations;
import org.rdswitchboard.libraries.graph.GraphNode;
import org.rdswitchboard.libraries.graph.GraphUtils;
import org.rdswitchboard.utils.neo4j.local.Neo4jUtils;

public class App {
	private static final String PROPERTIES_FILE = "properties/copy_harmonized.properties";
	private static final String SOURCE_NEO4J_FOLDER = "neo4j"; 
	private static final String TARGET_NEO4J_FOLDER = "neo4j1";
	
    private static final String PART_PROTOCOL = "://";
    private static final String PART_SLASH = "/";
    private static final String PART_WWW = "www.";
    private static final String PART_ORCID_URI = "orcid.org/";
    private static final String PART_DOI_PERFIX = "doi:";
    private static final String PART_DOI_URI = "dx.doi.org/";
	
	private static final Set<String> SOURCES = new HashSet<String>();
	private static final Set<String> TYPES = new HashSet<String>();
	private static final Map<String, Index<Node>> mapIndexes = new HashMap<String, Index<Node>>();
	private static final Map<Long, Long> mapKeys = new HashMap<Long, Long>();
	
	private static GraphDatabaseService srcGraphDb;
	private static GraphDatabaseService dstGraphDb;
	
	private static final RelationshipType relKnownAs = DynamicRelationshipType.withName(GraphUtils.RELATIONSHIP_KNOWN_AS);
	
	public static void main(String[] args) {
		try {
			String propertiesFile = PROPERTIES_FILE;
	        if (args.length > 0 && !StringUtils.isEmpty(args[0])) 
	        	propertiesFile = args[0];
	
	        Properties properties = new Properties();
	        try (InputStream in = new FileInputStream(propertiesFile)) {
	            properties.load(in);
	        }
	        
	        System.out.println("Export Neo4j database");
	                	        
	        String srcNeo4j1Folder = properties.getProperty("source.neo4j", SOURCE_NEO4J_FOLDER);
	        if (StringUtils.isEmpty(srcNeo4j1Folder))
	            throw new IllegalArgumentException("Neo4j1 Folder can not be empty");
	        System.out.println("Source Neo4j Folder: " + srcNeo4j1Folder);

	        String dstNeo4j2Folder = properties.getProperty("target.neo4j", TARGET_NEO4J_FOLDER);
	        if (StringUtils.isEmpty(dstNeo4j2Folder))
	            throw new IllegalArgumentException("Neo4j2 Folder can not be empty");
	        System.out.println("Target Neo4j Folder: " + dstNeo4j2Folder);
	        
	        System.out.println("Connecting to source database");
	        
	        srcGraphDb = Neo4jUtils.getReadOnlyGraphDb(srcNeo4j1Folder);
	        
	        System.out.println("Connecting to destination database");
	        dstGraphDb = Neo4jUtils.getGraphDb(dstNeo4j2Folder);
	        
	        GlobalGraphOperations global = Neo4jUtils.getGlobalOperations(srcGraphDb);
	        
	        long nodeCounter = 0;
	        long relCounter = 0;
	        	        
	        SOURCES.add(GraphUtils.SOURCE_DRYAD);
	        SOURCES.add(GraphUtils.SOURCE_ORCID);
	        SOURCES.add(GraphUtils.SOURCE_WEB);
	        SOURCES.add(GraphUtils.SOURCE_FIGSHARE);
	        SOURCES.add(GraphUtils.SOURCE_CROSSREF);
	        SOURCES.add(GraphUtils.SOURCE_ARC);
	        SOURCES.add(GraphUtils.SOURCE_NHMRC);
	        SOURCES.add(GraphUtils.SOURCE_ANDS);
	        	        
	        TYPES.add(GraphUtils.TYPE_INSTITUTION);
	        TYPES.add(GraphUtils.TYPE_PUBLICATION);
	        TYPES.add(GraphUtils.TYPE_RESEARCHER);
	        TYPES.add(GraphUtils.TYPE_DATASET);
	        TYPES.add(GraphUtils.TYPE_GRANT);
	        
/*	        mapKeys.put(GraphUtils.TYPE_INSTITUTION, new HashMap<Long, Long>());
	        mapKeys.put(GraphUtils.TYPE_PUBLICATION, new HashMap<Long, Long>());
	        mapKeys.put(GraphUtils.TYPE_RESEARCHER, new HashMap<Long, Long>());
	        mapKeys.put(GraphUtils.TYPE_DATASET, new HashMap<Long, Long>());
	        mapKeys.put(GraphUtils.TYPE_GRANT, new HashMap<Long, Long>());
*/	    	
	        
	        System.out.println("Create Indexes");
	        try ( Transaction tx = dstGraphDb.beginTx() ) {
		        for (String label : SOURCES) { 
		        	Neo4jUtils.createConstrant(dstGraphDb, DynamicLabel.label(label), GraphUtils.PROPERTY_KEY);
		        }
		        for (String label : TYPES) { 
		        	Neo4jUtils.createConstrant(dstGraphDb, DynamicLabel.label(label), GraphUtils.PROPERTY_KEY);
		        }
		        
	        	tx.success();
	        }
	        
	        try ( Transaction tx = dstGraphDb.beginTx() ) {
		        for (String label : SOURCES) { 
		        	mapIndexes.put(label, Neo4jUtils.getNodeIndex(dstGraphDb, label));
		        }
		        for (String label : TYPES) { 
		        	mapIndexes.put(label, Neo4jUtils.getNodeIndex(dstGraphDb, label));
		        }
		        
	        	tx.success();
	        }
	        
	        System.out.println("Copy Nodes");
	        try ( Transaction ignored = srcGraphDb.beginTx() ) 
			{
	        	long chunkSize = 0;
	        	long chunkCnt = 0;
	        	Transaction tx = dstGraphDb.beginTx();
	        	try {
		        	ResourceIterable<Node> srcNodes = global.getAllNodes();
		        	for (Node srcNode : srcNodes) {
		        		if (exportNode(srcNode))
		        			++chunkSize;
		        
		        		if (chunkSize > 1000) {
        					
		        			nodeCounter += chunkSize;
        					chunkSize = 0;
        					++chunkCnt;

        					System.out.println("Writing " + chunkCnt + " nodes chunk to database");
        				
        					tx.success();
        					tx.close();
        					tx = dstGraphDb.beginTx();			        					
        				}
		        	}
		        	
		        	nodeCounter += chunkSize;
		        	tx.success();
	        	} finally {
	        		tx.close();
	        	}
	        	
	        	System.out.println("Copy Relationships");
	        	
	        	chunkSize = 0;
	        	chunkCnt = 0;
	        	tx = dstGraphDb.beginTx();
	        	try {
		        	ResourceIterable<Node> srcNodes = global.getAllNodes();
		        	for (Node srcNode : srcNodes) {
		        		chunkSize += exportNodeRelationships(srcNode);
		        
		        		if (chunkSize > 1000) {
        					
		        			relCounter += chunkSize;
        					chunkSize = 0;
        					++chunkCnt;

        					System.out.println("Writing " + chunkCnt + " relationships chunk to database");
        				
        					tx.success();
        					tx.close();
        					tx = dstGraphDb.beginTx();			        					
        				}
		        	}
		        	
		        	nodeCounter += chunkSize;
		        	tx.success();
	        	} finally {
	        		tx.close();
	        	}
			}
	        
	        System.out.println("Done. Created " + nodeCounter + " nodes and " + relCounter + " relationships");
	        
	      	
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static boolean exportNode(Node srcNode) throws Exception {
		// a simple check to see if we already have this node
		long nId = srcNode.getId();
		if (mapKeys.containsKey(nId))
			return false;
		
		// query all nodes, bound by knownAs relationship
		Map<Long, Node> nodes = getKnownAs(srcNode, new HashMap<Long, Node>());
		if (!nodes.isEmpty()) {
			GraphNode graphNode = new GraphNode();
			
			Collection<Node> srcNodes = nodes.values();
			
			// export node by priority
			exportGraphNode(graphNode, srcNodes, GraphUtils.SOURCE_ANDS, GraphUtils.PROPERTY_RDS_URL);
			exportGraphNode(graphNode, srcNodes, GraphUtils.SOURCE_ARC, GraphUtils.PROPERTY_URL);
			exportGraphNode(graphNode, srcNodes, GraphUtils.SOURCE_NHMRC, GraphUtils.PROPERTY_URL);
			exportGraphNode(graphNode, srcNodes, GraphUtils.SOURCE_ORCID, GraphUtils.PROPERTY_KEY);
			exportGraphNode(graphNode, srcNodes, GraphUtils.SOURCE_DRYAD, GraphUtils.PROPERTY_KEY);
			exportGraphNode(graphNode, srcNodes, GraphUtils.SOURCE_CROSSREF, GraphUtils.PROPERTY_KEY);
			exportGraphNode(graphNode, srcNodes, GraphUtils.SOURCE_FIGSHARE, GraphUtils.PROPERTY_KEY);
			exportGraphNode(graphNode, srcNodes, GraphUtils.SOURCE_WEB, GraphUtils.PROPERTY_KEY);
			
			// check what node has a key
			if (graphNode.hasKey()) {
				String key = (String) graphNode.getKey();
			//	System.out.println(key);
				String type = (String) graphNode.getType();
				Object sources = graphNode.getSource();
				
				Label lType = DynamicLabel.label(type);
				
				// check if we already have this node in the database
				Node dstNode = findSingleNode(lType, key);
				if (null == dstNode) {
					// create new node if needed and add it to the index
					dstNode = dstGraphDb.createNode();
					dstNode.setProperty(GraphUtils.PROPERTY_KEY, key);
					
					mapIndexes.get(type).add(dstNode, GraphUtils.PROPERTY_KEY, key);
				}

				long dstId = dstNode.getId();
				for (Long nodeId : nodes.keySet())
					mapKeys.put(nodeId, dstId);
				
				// copy missing data	
				for (Map.Entry<String, Object> entry : graphNode.getProperties().entrySet()) {
					String property = entry.getKey();
					if (!property.equals(GraphUtils.PROPERTY_KEY) && !dstNode.hasProperty(property)) 
						dstNode.setProperty(property, entry.getValue());
				}
				
				// assing labels
				dstNode.addLabel(lType);
				
				if (sources instanceof String) 
					dstNode.addLabel(DynamicLabel.label((String) sources));
				else if (sources instanceof String[]) {
					for (String source : (String[]) sources)
						dstNode.addLabel(DynamicLabel.label(source));
				}
				
				return true;
			}				
		}
		
		return false;
	}
	
	private static int exportNodeRelationships(Node srcNode) throws Exception {
		int counter = 0;
		Long startId = mapKeys.get(srcNode.getId());
		if (null != startId) {
			Iterable<Relationship> rels = srcNode.getRelationships(Direction.OUTGOING);
			for (Relationship rel : rels) {
				RelationshipType type = rel.getType();
				Long endId = mapKeys.get(rel.getEndNode().getId());
				if (null != endId && !startId.equals(endId)) {
					Node startNode = dstGraphDb.getNodeById(startId);
					boolean exists = false;
					
					Iterable<Relationship> rels2 = startNode.getRelationships(type, Direction.OUTGOING);
					for (Relationship rel2 : rels2) {
						if (rel2.getEndNode().getId() == endId) {
							exists = true;
							break;
						}							
					}
					
					if (!exists) {
						Node endNode = dstGraphDb.getNodeById(endId);
						startNode.createRelationshipTo(endNode, type);
						
						++counter;
					}
				}			
			}
		}
		
		return counter;
	}
	
	private static void exportGraphNode(GraphNode graphNode, Collection<Node> nodes, 
			String source, String keyName) throws Exception {
		for (Node node : nodes) {
			if (source.equals(node.getProperty(GraphUtils.PROPERTY_SOURCE))) {				
				if (!graphNode.hasKey() && node.hasProperty(keyName)) {
					String key = extractUri((String) node.getProperty(keyName));
					graphNode.setKey(key);
				}
				
				String nodeSource = (String) node.getProperty(GraphUtils.PROPERTY_SOURCE);
				graphNode.addSource(nodeSource);
				
				String nodeType = (String) node.getProperty(GraphUtils.PROPERTY_TYPE);
				if (!graphNode.hasType()) 
					graphNode.setType(nodeType);
	/*			else
					if (!nodeType.equals(graphNode.getType()))
						throw new Exception("Unable to megre nodes with different types: " + nodeType + ", " + graphNode.getType());*/

				
				for (String property : node.getPropertyKeys()) {
					if (!property.equals(keyName) 
							&& !property.equals(GraphUtils.PROPERTY_KEY)
							&& !property.equals(GraphUtils.PROPERTY_SOURCE)
							&& !property.equals(GraphUtils.PROPERTY_TYPE)) {
						graphNode.setPropertyOnce(property, node.getProperty(property));
					}
				}
			}			
		}
	}
	
	
	public static String extractUri(String str) {
    	if (null != str) {
    		str = str.trim();
    		if (!str.isEmpty()) {
	    		int index = str.indexOf(PART_PROTOCOL);
		    	if (index >= 0)
		    		str = str.substring(index + PART_PROTOCOL.length());
		    	if (str.startsWith(PART_WWW))
		    		str = str.substring(PART_WWW.length());
		    	if (str.endsWith(PART_SLASH))
		    		str = str.substring(0, str.length()-1);

		    	if (!str.isEmpty())
		    		return str;
    		}
    	}
    	
    	return null;
    }
	
	private static Map<Long, Node> getKnownAs(Node node, Map<Long, Node> map) {
		// make sure the node has valid source
		String source = (String) node.getProperty(GraphUtils.PROPERTY_SOURCE);
		if (SOURCES.contains(source)) {
			// make sure the node has valid type
			String type = (String) node.getProperty(GraphUtils.PROPERTY_TYPE);
			if (TYPES.contains(type)) {
				// add the node to the map
				map.put(node.getId(), node);
				
				// query all node relationships
				Iterable<Relationship> rels = node.getRelationships(relKnownAs);
				for (Relationship rel : rels) {
					// find node sitting on other end of relationship
					Node other = rel.getOtherNode(node);
					// check what we haven't seen that node before
					if (!map.containsKey(other.getId())) 
						getKnownAs(other, map);
				}
			}				
		}
				
		return map;
	}
	
	public static Node findSingleNode(Label label, Object key) {
		try (ResourceIterator<Node> hits = dstGraphDb.findNodes(label, GraphUtils.PROPERTY_KEY, key)) {
			if (hits.hasNext())
				return hits.next();
		}
		
		return null;
	}
}
