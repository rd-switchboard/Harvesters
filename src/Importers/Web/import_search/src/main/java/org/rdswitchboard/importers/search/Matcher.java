package org.rd.switchboard.importers.search;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.rd.switchboard.utils.google.cache2.GoogleUtils;
import org.rd.switchboard.utils.graph.GraphConnection;
import org.rd.switchboard.utils.graph.GraphNode;
import org.rd.switchboard.utils.graph.GraphRelationship;
import org.rd.switchboard.utils.neo4j.Neo4jUtils;
import org.rd.switchboard.utils.aggrigation.AggrigationUtils;

public class Matcher {
	private final String key;
	private final File cacheFile;
	private final File metadataFile;
	private final File nodesFile;
	private final File relationshipFile;
	private final Map<String, List<GraphConnection>> nodes;
	
	private static final String PROPERTY_SOURCE_SEARCH = "source_search";
	private static final boolean VALUE_TRUE = true;	
	
	private static Map<String, Object> pars = new HashMap<String, Object>();
	static {
		pars.put(PROPERTY_SOURCE_SEARCH, VALUE_TRUE);
	}
	
	public Matcher(String key, 
			File cacheFile, File metadataFile, 
			File nodesFile, File relationshipFile,
			Map<String, List<GraphConnection>> nodes) {
		this.key = key;
		this.cacheFile = cacheFile;
		this.nodesFile = nodesFile;
		this.relationshipFile = relationshipFile;
		this.metadataFile = metadataFile;
		this.nodes = nodes;
	}
	
	public void run() {
		try {
			List<GraphNode> graphNodes = null;
			List<GraphRelationship> graphRelationships = null;
			
			String cacheData = FileUtils.readFileToString(cacheFile);
			cacheData = StringEscapeUtils.unescapeHtml(cacheData)
					.toLowerCase()				// convert to lower case
					.replaceAll("\u00A0", " "); // replace all long spaces with simple space
			
			GraphConnection end = null;
			//long beginTime = System.currentTimeMillis();
			for (Map.Entry<String, List<GraphConnection>> entry : nodes.entrySet()) {
				if (cacheData.contains(entry.getKey())) {
					
				//	System.out.println("Found matching URL: " + key + " for needle: " + entry.getKey());
	//				logSearch(entry.getKey());
					
					if (null == end) {
						end = new GraphConnection(AggrigationUtils.LABEL_WEB, AggrigationUtils.LABEL_RESEARCHER, key);
	
						String author = null;
						if (null != metadataFile) 
							author = GoogleUtils.getMetatag(metadataFile, GoogleUtils.METDATA_DC_TITLE);											
						
						Map<String, Object> map = new HashMap<String, Object>();
						map.put(Neo4jUtils.PROPERTY_KEY, key);
						map.put(Neo4jUtils.PROPERTY_NODE_SOURCE, AggrigationUtils.LABEL_WEB);
						map.put(Neo4jUtils.PROPERTY_NODE_TYPE, AggrigationUtils.LABEL_RESEARCHER);
						map.put(Neo4jUtils.PROPERTY_URL, key);
						if (null != author)
							map.put(AggrigationUtils.PROPERTY_NAME, author);
						
						GraphNode node = new GraphNode(map);
						
						graphNodes = new ArrayList<GraphNode>();
						graphNodes.add(node);
					}
					
					for (GraphConnection start : entry.getValue()) {
						// The node id has been loaded from the actuall neo4j so it mast exist
	
						GraphRelationship graphRelationship = new GraphRelationship(
								AggrigationUtils.RELATIONSHIP_RELATED_TO, pars, start, end);
						
						if (null == graphRelationships)
							graphRelationships = new ArrayList<GraphRelationship>();
						graphRelationships.add(graphRelationship);
					}
				}
			}
				
			if (null != graphNodes)
				GoogleUtils.mapper.writeValue(nodesFile, graphNodes);
			if (null != graphRelationships)
				GoogleUtils.mapper.writeValue(relationshipFile, graphRelationships);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
