package org.rd.switchboard.importers.institutions;

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
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Institutions Importer class
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class Importer {
	
	private static final String LABEL_INSTITUTION = "Institution";
	private static final String LABEL_WEB = "Web";
	private static final String LABEL_WEB_INSTITUTION = LABEL_WEB + "_" + LABEL_INSTITUTION;
	
	private static final String PROPERTY_KEY = "key"; 
	private static final String PROPERTY_NODE_SOURCE = "node_source";
	private static final String PROPERTY_NODE_TYPE = "node_type";
	private static final String PROPERTY_COUNTRY = "country";
	private static final String PROPERTY_STATE = "state";
	private static final String PROPERTY_TITLE = "title";
	private static final String PROPERTY_URL = "url";
	private static final String PROPERTY_HOST = "host";
	
	private RestAPI graphDb;
	private RestCypherQueryEngine engine;
	private RestIndex<Node> indexWebInstitution;
//	private RestIndex<Node> indexRecord;
	
	private Label labelInstitution = DynamicLabel.label(LABEL_INSTITUTION);
	private Label labelWeb = DynamicLabel.label(LABEL_WEB);
	
	/**
	 * Class constructor. 
	 * @param neo4jUrl An URL to the Neo4J
	 */
	public Importer(final String neo4jUrl) {
		graphDb = new RestAPIFacade(neo4jUrl);
		engine = new RestCypherQueryEngine(graphDb);  
		
		engine.query("CREATE CONSTRAINT ON (n:" + LABEL_WEB_INSTITUTION + ") ASSERT n." + PROPERTY_KEY + " IS UNIQUE", Collections.<String, Object> emptyMap());
		
		indexWebInstitution = graphDb.index().forNodes(LABEL_WEB_INSTITUTION);

	}

	/**
	 * Function to import instititions from an CSV file.
	 * For every line in the file, except a header line, an instace of Web:Institution will be 
	 * created. Institution URL will be used as an unique node key. The nodes with the same key 
	 * will NOT be overwritten.
	 * @param institutionsCsv A path to institutions.csv file
	 */
	public void importInstitutions(final String institutionsCsv) {
		try 
		{
			CSVReader reader = new CSVReader(new FileReader(institutionsCsv));
		
			String[] grant;
			boolean header = false;
			while ((grant = reader.readNext()) != null) 
			{
				if (!header)
				{
					header = true;
					continue;
				}
				if (grant.length != 4)
					continue;
				
				String country = grant[0];
				String state = grant[1];
				String title = grant[2];
				String url = grant[3];
				
				System.out.println(title + " - " + url);
		
				if (null != url && !url.isEmpty()) {
				
					URL hostUrl = null;
					
					try {
						hostUrl = new URL(url);
					} catch(MalformedURLException ex) {
						hostUrl = new URL("http://" + url);
					}
					
					String host = hostUrl.getHost();
					if (host.startsWith("www."))
						host = host.substring(4);
					else if (host.startsWith("www3."))
						host = host.substring(5);
					else if (host.startsWith("web."))
						host = host.substring(4);
					
					Map<String, Object> map = new HashMap<String, Object>();
					map.put(PROPERTY_KEY, url);
					map.put(PROPERTY_NODE_SOURCE, LABEL_WEB);
					map.put(PROPERTY_NODE_TYPE, LABEL_INSTITUTION);
					if (null != state && !state.isEmpty())
						map.put(PROPERTY_STATE, state);
					map.put(PROPERTY_COUNTRY, country);
					map.put(PROPERTY_TITLE, title);
					map.put(PROPERTY_URL, url);
					map.put(PROPERTY_HOST, host);
					
					RestNode node = graphDb.getOrCreateNode(indexWebInstitution, PROPERTY_KEY, url, map);
					if (!node.hasLabel(labelInstitution))
						node.addLabel(labelInstitution); 
					if (!node.hasLabel(labelWeb))
						node.addLabel(labelWeb);
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
