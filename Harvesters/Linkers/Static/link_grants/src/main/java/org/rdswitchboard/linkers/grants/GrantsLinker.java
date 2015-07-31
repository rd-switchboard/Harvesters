package org.rdswitchboard.linkers.grants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.rdswitchboard.libraries.graph.GraphUtils;
import org.rdswitchboard.utils.google.cache2.GoogleLink;
import org.rdswitchboard.utils.google.cache2.GoogleResult;
import org.rdswitchboard.utils.google.cache2.GoogleUtils;
import org.rdswitchboard.utils.google.cse.Item;
import org.rdswitchboard.utils.google.cse.Query;
import org.rdswitchboard.utils.google.cse.QueryResponse;
import org.rdswitchboard.utils.neo4j.local.Neo4jUtils;

public class GrantsLinker {
	private GraphDatabaseService graphDb;

	private RelationshipType relKnownAs = DynamicRelationshipType.withName( GraphUtils.RELATIONSHIP_KNOWN_AS );
	
	private boolean verbose = false;
	
	public GrantsLinker(final String neo4jFolder) {
		graphDb = Neo4jUtils.getGraphDb( neo4jFolder );
	}
	
	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	public void link() throws Exception {
		Map<String, Set<Long>> nodes = new HashMap<String, Set<Long>>();
		
		loadNodes( nodes, GraphUtils.SOURCE_ANDS, GraphUtils.TYPE_GRANT, 
				GraphUtils.PROPERTY_TITLE, filterHas(GraphUtils.PROPERTY_PURL) );
		
		processNodes(nodes, googleCache, CACHE_GRANT);
		
		// add publication linking here
	}
	
	private void processGrants(String source) {

		String cypher = "MATCH (n:" + source + ":" + type + ")";
		if (null != filter)
			cypher += " WHERE " + filter;
		cypher += " RETURN ID(n) AS " + FIELD_ID + ", n." + fieldTitle + " AS " + FIELD_TITLE;
				
		try ( Transaction ignored = graphDb.beginTx();
       	      Result result = graphDb.execute( cypher ) ) {
    	    while ( result.hasNext() ) {
    	        Map<String,Object> row = result.next();
    	        long nodeId = (long) (Integer) row.get(FIELD_ID);
    	        String title = (String) row.get(FIELD_TITLE);
    	        
    	        if (null != title) {
    	        	title = title.trim().toLowerCase();
    	        	 if (title.length() > minTitleLength && !blackList.contains(title)) 
    	        		 putUnique(nodes, title, nodeId);
    	        
    	        }
    	    }
    	}    
	}
	
	private void processNodes(Map<String, Set<Long>> nodes, String googleCache, String folderName) throws Exception {
		System.out.println("Processing cached pages");
		
		googleQuery.setJsonFolder(GoogleUtils.getJsonFolder(googleCache).toString());
		
		File cache = GoogleUtils.getCacheFolder(googleCache, folderName);
		File[] files = cache.listFiles();
		for (File file : files) 
			if (!file.isDirectory())
				try {
					GoogleResult result = (GoogleResult) jaxbUnmarshaller.unmarshal(file);
					if (result != null) {
						String text = result.getText();
						Set<Long> nodeIds = nodes.get(text.trim().toLowerCase());
						if (null != nodeIds) 
							for (String link : result.getLinks()) 
								if (isLinkFollowAPattern(link)) {
									System.out.println("Found matching URL: " + link + " for grant: " + text);
								
									Node nodeResearcher = getOrCreateWebResearcher(link, text);
									for (Long nodeId : nodeIds) 
										Neo4jUtils.createUniqueRelationship(graphDb.getNodeById(nodeId), 
												nodeResearcher, relRelatedTo, Direction.OUTGOING, null);	
								}						
					}							
				} catch (JAXBException e) {
					e.printStackTrace();
				}
	}	
	
	private void putUnique(Map<String, Set<Long>> nodes, String key, Long id) {
        if (nodes.containsKey(key))
            nodes.get(key).add(id);
        else {
            Set<Long> set = new HashSet<Long>();
            set.add(id);

            nodes.put(key, set);
        }
	}
	
	private String filterHas(String field) {
		return  "HAS (n." + field + ")";
	}
	
	private boolean isLinkFollowAPattern(String link) {
        for (Pattern pattern : webPatterns) 
                if (pattern.matcher(link).find())
                        return true;
        return false;
    }
	
	private Node findWebResearcher(String url) {
		IndexHits<Node> hits = indexWeb.get(GraphUtils.PROPERTY_KEY, url);
		if (null != hits && hits.hasNext())
			return hits.getSingle();
		
		return null;
	}
	
	private Node getOrCreateWebResearcher(String url, String searchString) throws Exception {
		Node node = findWebResearcher(url);
		if (null != node) {
			if (!node.hasLabel(labelResearcher))
				throw new Exception("The node defined by URL: " + url + " is not a Researcher node");
			
			return node;
		}
			
		node = graphDb.createNode();
		node.setProperty(GraphUtils.PROPERTY_KEY, url);
		node.setProperty(GraphUtils.PROPERTY_SOURCE, GraphUtils.SOURCE_WEB);
		node.setProperty(GraphUtils.PROPERTY_TYPE, GraphUtils.TYPE_RESEARCHER);
		node.setProperty(GraphUtils.PROPERTY_URL, url);
		
		String author = getAuthor(url, searchString);
		if (null != author)
			node.setProperty(GraphUtils.PROPERTY_TITLE, author);
		
		node.addLabel(labelWeb);
		node.addLabel(labelResearcher);
		
		indexWeb.add(node, GraphUtils.PROPERTY_KEY, url);

		return node;
	}
	
	private Map<String, Object> getPageMap(String link, String searchString) {
		QueryResponse response = googleQuery.queryCache(searchString);
		if (null != response) 
			for (Item item : response.getItems()) 
				if (item.getLink().equals(link)) 
					return item.getPagemap();

		return null;
	}
	
	private String getAuthor(String link, String searchString) {
		try {
			Map<String, Object> pagemap = getPageMap(link, searchString);
			if (null != pagemap) {
				 @SuppressWarnings("unchecked")
				 List<Object> metatags = (List<Object>) pagemap.get("metatags");
				 if (null != metatags && metatags.size() > 0) {
					 @SuppressWarnings("unchecked")
					 Map<String, Object> metatag = (Map<String, Object>) metatags.get(0);
					 if (null != metatag) {
						 String dcTitle = (String) metatag.get("dc.title");
						 String citationAuthor = (String) metatag.get("citation_author");
						 
						 if (null != dcTitle) {
							 System.out.println("Found dc.title: " + dcTitle);	

							 return dcTitle;
						 } 
						 
						 if (null != citationAuthor) {
							 System.out.println("Found citation_author: " + citationAuthor);	
							 
							 return citationAuthor;
						 }
						 	
						 System.out.println("Unable to find author information in metatag");																 
					 }
				 }
			 }
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		 
		return null;
	}
}
