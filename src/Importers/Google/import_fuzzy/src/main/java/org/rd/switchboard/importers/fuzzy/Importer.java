package org.rd.switchboard.importers.fuzzy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.rd.switchboard.utils.google.cache.Grant;
import org.rd.switchboard.utils.google.cache.Page;
import org.rd.switchboard.utils.google.cache.Publication;
import org.rd.switchboard.utils.neo4j.Neo4jUtils;
import org.rd.switchboard.utils.aggrigation.AggrigationUtils;
import org.rd.switchboard.utils.fuzzy_search.FuzzySearch;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

public class Importer {

	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	private RestIndex<Node> indexWebResearcher;
	
	private JAXBContext jaxbContext;
	private Unmarshaller jaxbUnmarshaller;
		
	private PrintWriter logger;
	private PrintWriter fuzzy;
	
	private enum Labels implements Label {
		Web, Dryad, RDA, Researcher, Pattern, Publication, Grant
	}
	
	private enum RelTypes implements RelationshipType {
		relatedTo
	}
	
	private static final String FOLDER_CACHE = "cache";
	private static final String FOLDER_PAGE = "page";
	private static final String PROPERTY_SOURCE_FUZZY = "source_fuzzy";
	
	private static final boolean VALUE_TRUE = true;
		
	private List<Pattern> webPatterns;
	private Set<String> blackList;
	
	private Map<String, Set<Long>> nodes = new HashMap<String, Set<Long>>();

	public Importer(String neo4jUrl) throws FileNotFoundException, UnsupportedEncodingException, JAXBException {
		graphDb = new RestAPIFacade(neo4jUrl);
		engine = new RestCypherQueryEngine(graphDb);  
		
		Neo4jUtils.createConstraint(engine, Labels.Web, Labels.Researcher);
		indexWebResearcher = Neo4jUtils.getIndex(graphDb, Labels.Web, Labels.Researcher);
				
		jaxbContext = JAXBContext.newInstance(Publication.class, Grant.class, Page.class);
		jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		
		logger = new PrintWriter("import_fuzzy.log", StandardCharsets.UTF_8.name());
		fuzzy = new PrintWriter("fuzzy_search.log", StandardCharsets.UTF_8.name());
	}
	
	/**
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * 
	 */
	public void init(String blackList) throws FileNotFoundException, IOException {
		this.blackList = AggrigationUtils.loadBlackList(blackList);
		this.webPatterns = AggrigationUtils.loadWebPatterns(engine);
		
		AggrigationUtils.loadDryadPublications(engine, nodes, this.blackList);
		AggrigationUtils.loadRdaGrants(engine, nodes, this.blackList);
	}
	
	/**
	 * 
	 * @param googleCache
	 */
	
	public void process(String googleCache) {
		log ("Processing cached publications");

	//	googleQuery.setJsonFolder(new File(googleCache, FOLDER_JSON).getPath());

		Map<String, Object> pars = new HashMap<String, Object>();
		pars.put(PROPERTY_SOURCE_FUZZY, VALUE_TRUE);

		File pages = new File (googleCache, FOLDER_CACHE + "/" + FOLDER_PAGE);
		File[] files = pages.listFiles();
		for (File file : files) 
			if (!file.isDirectory())
				try {
					Page page = (Page) jaxbUnmarshaller.unmarshal(file);
					if (page != null && AggrigationUtils.isLinkFollowAPattern(webPatterns, page.getLink())) {
						log ("Processing URL: " + page.getLink());
						File cacheFile = new File("google/" + page.getCache()); // temporary solution
						if (cacheFile.exists() && !cacheFile.isDirectory()) {
							String cacheData = FileUtils.readFileToString(cacheFile);
							cacheData = StringEscapeUtils.unescapeHtml(cacheData)
									.toLowerCase()				// convert to lower case
									.replaceAll("\u00A0", " "); // replace all long spaces with simple space
							
							final char[] data = FuzzySearch.stringToCharArray(cacheData);
							
							for (Map.Entry<String, Set<Long>> entry : nodes.entrySet()) {
								if (FuzzySearch.find(
										FuzzySearch.stringToCharArray(entry.getKey()), 
											data, 
											AggrigationUtils.getDistance(entry.getKey().length())) >= 0) {
									
									log ("Found matching URL: " + page.getLink() + " for needle: " + entry.getKey());
									logFuzzy(entry.getKey());
									
									RestNode nodeResearcher = AggrigationUtils.findWebResearcher(indexWebResearcher, page.getLink());
									if (null != nodeResearcher)
										for (Long nodeId : entry.getValue()) {
											RestNode nodePublication = graphDb.getNodeById(nodeId);
										
											Neo4jUtils.createUniqueRelationship(graphDb, nodePublication, nodeResearcher, 
													RelTypes.relatedTo, Direction.OUTGOING, pars);
										}
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
		
	}	

	private void log(String message) {
		System.out.println(message);
		
		logger.println(message);  	
	}
	
	private void logFuzzy(String needle) {
		fuzzy.println(needle);  	
	}
}
