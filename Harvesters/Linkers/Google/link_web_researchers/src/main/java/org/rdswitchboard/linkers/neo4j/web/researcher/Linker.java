package org.rdswitchboard.linkers.neo4j.web.researcher;

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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.rdswitchboard.libraries.graph.GraphUtils;
import org.rdswitchboard.libraries.neo4j.Neo4jUtils;
import org.rdswitchboard.utils.google.cache2.Link;
import org.rdswitchboard.utils.google.cache2.Result;
import org.rdswitchboard.utils.google.cache2.GoogleUtils;
import org.rdswitchboard.utils.google.cse.Item;
import org.rdswitchboard.utils.google.cse.Query;
import org.rdswitchboard.utils.google.cse.QueryResponse;

public class Linker {	
	private static final String FIELD_PATTERN = "pattern";
	private static final String FIELD_ID = "id";
	private static final String FIELD_TITLE = "title";
	
	private static final String CACHE_PUBLICATION = "publication";
	private static final String CACHE_GRANT = "grant";
	
	private GraphDatabaseService graphDb;
	private Index<Node> indexWeb;
	private Label labelWeb = DynamicLabel.label( GraphUtils.SOURCE_WEB );
	private Label labelResearcher = DynamicLabel.label( GraphUtils.TYPE_RESEARCHER );
	private RelationshipType relRelatedTo = DynamicRelationshipType.withName( GraphUtils.RELATIONSHIP_RELATED_TO );
	
	private boolean verbose = false;
	
	private Set<String> blackList;
	private List<Pattern> webPatterns;
	
	private Query googleQuery;
	
	private JAXBContext jaxbContext;
	private Unmarshaller jaxbUnmarshaller;
	
	private final int minTitleLength;
	
	public Linker(final String neo4jFolder, final String blackList, final int minTitleLength, boolean verbose) throws FileNotFoundException, IOException, JAXBException {
		this.minTitleLength = minTitleLength;
		this.verbose = verbose;
		
		jaxbContext = JAXBContext.newInstance(Link.class, Result.class);
		jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		
		graphDb = Neo4jUtils.getGraphDb( neo4jFolder );
		
		googleQuery = new Query(null, blackList);
		
		try ( Transaction tx = graphDb.beginTx() ) {
			indexWeb = Neo4jUtils.getNodeIndex(graphDb, GraphUtils.SOURCE_WEB);
		}
		
		loadBlackList( blackList );
		loadWebPatterns();
	}
	
	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	public void link(String googleCache) throws Exception {
		Map<String, Set<Long>> nodes = new HashMap<String, Set<Long>>();
		
		if (verbose)
			System.out.println("Loading Nodes");
		
		loadNodes( nodes, GraphUtils.SOURCE_ANDS, GraphUtils.TYPE_GRANT, 
				GraphUtils.PROPERTY_TITLE, filterHas(GraphUtils.PROPERTY_PURL) );
		/*loadNodes( nodes, GraphUtils.SOURCE_ARC, GraphUtils.TYPE_GRANT, 
				GraphUtils.PROPERTY_TITLE, filterHas(GraphUtils.PROPERTY_PURL) );
		loadNodes( nodes, GraphUtils.SOURCE_NHMRC, GraphUtils.TYPE_GRANT, 
				GraphUtils.PROPERTY_TITLE, filterHas(GraphUtils.PROPERTY_PURL) );*/
		
		if (verbose)
			System.out.println("Done. Loaded " + nodes.size() + " Nodes\nProcessing Nodes");
				
		try ( Transaction tx = graphDb.beginTx() ) {
			processNodes(nodes, googleCache, CACHE_GRANT);
			
			tx.success();
		}
		
		// add publication linking here
	}
	
	private void loadBlackList(String blackList) throws FileNotFoundException, IOException {
		if (verbose)
			System.out.println("Loading Black List");
				
		this.blackList = new HashSet<String>();
        
        try(BufferedReader br = new BufferedReader(new FileReader(new File(blackList)))) {
            for(String line; (line = br.readLine()) != null; ) {
            	String s = line.trim().toLowerCase();
            	
            	if (verbose)
            		System.out.println("Black List: " + s);
            	
            	this.blackList.add(s);
            }            
        }
    }
	
	private void loadWebPatterns() {
		if (verbose)
			System.out.println("Loading Web Patterns");
		
        this.webPatterns = new ArrayList<Pattern>();

        String cypher = "MATCH (n:" + GraphUtils.SOURCE_WEB 
	    		  + ":" + GraphUtils.TYPE_PATTERN 
		          + ") WHERE HAS(n." + GraphUtils.PROPERTY_PATTERN 
		          + ") RETURN n." + GraphUtils.PROPERTY_PATTERN 
		          + " AS " + FIELD_PATTERN;
        
        try ( Transaction ignored = graphDb.beginTx();
        	org.neo4j.graphdb.Result result = graphDb.execute( cypher ) ) {
    	    while ( result.hasNext() ) {
    	        Map<String,Object> row = result.next();
    	        String pattern = ((String) row.get(FIELD_PATTERN));
    	        if (verbose) 
    	        	System.out.println("Pattern: " + pattern);
    	        
    	        if (null != pattern) 
	   	        	this.webPatterns.add(Pattern.compile(pattern));
    	    }
    	}    
	}
	
	private void loadNodes(Map<String, Set<Long>> nodes, String source, String type, 
			String fieldTitle, String filter) {

		if (verbose)
			System.out.println("Source: " + source + ", Type: " + type + ", Field: " + fieldTitle + ", Filter: " + filter);
		
		String cypher = "MATCH (n:" + source + ":" + type + ")";
		if (null != filter)
			cypher += " WHERE " + filter;
		cypher += " RETURN ID(n) AS " + FIELD_ID + ", n." + fieldTitle + " AS " + FIELD_TITLE;
				
		try ( Transaction ignored = graphDb.beginTx();
			org.neo4j.graphdb.Result result = graphDb.execute( cypher ) ) {
    	    while ( result.hasNext() ) {
    	        Map<String,Object> row = result.next();
    	        long nodeId = (Long) row.get(FIELD_ID);
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
		if (verbose)
			System.out.println("Processing cached pages: " + folderName);
		
		googleQuery.setJsonFolder(GoogleUtils.getJsonFolder(googleCache).toString());
		
		File linksFolder = GoogleUtils.getLinkFolder(googleCache);
		File cacheFolder = GoogleUtils.getCacheFolder(googleCache, folderName);
		File[] files = cacheFolder.listFiles();
		for (File file : files) 
			if (!file.isDirectory())
				try {
					if (verbose)
						System.out.println("Processing file: " + file.toString());
					
					Result result = (Result) jaxbUnmarshaller.unmarshal(file);
					if (result != null) {
						String text = result.getText();
						if (verbose)
							System.out.println("Searching for a string: " + text);
						Set<Long> nodeIds = nodes.get(text.trim().toLowerCase());
						if (null != nodeIds) { 
							if (verbose)
								System.out.println("Found " + nodeIds.size() + " possible matches");

							for (String l : result.getLinks()) {
								Link link = (Link) jaxbUnmarshaller.unmarshal(new File(linksFolder, l));
								if (null != link) {
									
									if (verbose)
										System.out.println("Testing link: " + link.getLink());
									if (isLinkFollowAPattern(link.getLink())) {
										if (verbose)
											System.out.println("Found matching URL: " + link.getLink() + " for grant: " + text);
									
										Node nodeResearcher = getOrCreateWebResearcher(link.getLink(), text);
										for (Long nodeId : nodeIds) 
											Neo4jUtils.createUniqueRelationship(graphDb.getNodeById(nodeId), 
													nodeResearcher, relRelatedTo, Direction.OUTGOING, null);	
									}
								}
							}
						}
					} else
						throw new Exception("Unable to parse a file: " + file.toString());
				} catch (Exception e) {
					e.printStackTrace();
					
					break;
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
		url = GraphUtils.extractFormalizedUrl(url);
		
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
