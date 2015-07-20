package org.rdswitchboard.importers.services;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Importer class
 * 
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class Importer {
	private static final String LABEL_SERVICE = "Service";
	private static final String LABEL_WEB = "Web";
	private static final String LABEL_WEB_SERVICE = LABEL_WEB + "_" + LABEL_SERVICE;
	
	private static final String PROPERTY_KEY = "key"; 
	private static final String PROPERTY_NODE_SOURCE = "node_source";
	private static final String PROPERTY_NODE_TYPE = "node_type";
	private static final String PROPERTY_NAME = "name";
	private static final String PROPERTY_URL = "url";
	
	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	private RestIndex<Node> indexWebService;
//	private RestIndex<Node> indexRecord;
	private RestIndex<Node> indexService;
	
	private Label labelService = DynamicLabel.label(LABEL_SERVICE);
	private Label labelWeb = DynamicLabel.label(LABEL_WEB);
	
	/**
	 * Class constructor. 
	 * @param neo4jUrl An URL to the Neo4J
	 */
	public Importer(final String neo4jUrl) {
		graphDb = new RestAPIFacade(neo4jUrl);
		engine = new RestCypherQueryEngine(graphDb);  
		
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_WEB_SERVICE + ") ASSERT n." + PROPERTY_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		
		indexWebService = graphDb.index().forNodes(LABEL_WEB_SERVICE);
		indexService = graphDb.index().forNodes(LABEL_SERVICE);

	}

	/**
	 * Function to import services from an CSV file.
	 * For every line in the file, except a header line, an instace of Web:Service will be 
	 * created. The nodes with the same key will NOT be overwritten.
	 * 
	 * @param servicesCsv A path to services.csv file
	 */
	public void importServices(final String servicesCsv) {
		try 
		{
			CSVReader reader = new CSVReader(new FileReader(servicesCsv));
		
			String[] service;
			boolean header = false;
			while ((service = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (service.length != 2)
					continue;
				
				String name =service[0];
				String url = service[1];
				
				System.out.println(name + "," + url);
		
				if (null != url && !url.isEmpty()) {
				
					Map<String, Object> map = new HashMap<String, Object>();
					map.put(PROPERTY_KEY, url);
					map.put(PROPERTY_NODE_SOURCE, LABEL_WEB);
					map.put(PROPERTY_NODE_TYPE, LABEL_SERVICE);
					map.put(PROPERTY_NAME, name);
					map.put(PROPERTY_URL, url);
					
					RestNode node = graphDb.getOrCreateNode(indexWebService, PROPERTY_KEY, url, map);
					if (!node.hasLabel(labelService))
						node.addLabel(labelService); 
					if (!node.hasLabel(labelWeb))
						node.addLabel(labelWeb);
					
					IndexHits<Node> hitsUrl =indexService.get(PROPERTY_URL, node.getProperty(PROPERTY_URL));
					if (hitsUrl == null || hitsUrl.size() == 0)
						indexService.add(node, PROPERTY_URL, node.getProperty(PROPERTY_URL));
					
					IndexHits<Node> hitsName =indexService.get(PROPERTY_NAME, node.getProperty(PROPERTY_NAME));
					if (hitsName == null || hitsName.size() == 0)
						indexService.add(node, PROPERTY_NAME, node.getProperty(PROPERTY_NAME));
				}
			}
			
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
