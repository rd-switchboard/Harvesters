package org.rdswitchboard.importers.rda;

/**
 * Main class for RDA JSON data importer
 * 
 * This software design to process JSON records in rda/json
 * and will post data into Neo4J located at http://localhost:7474/db/data/
 * 
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.0.2
 * 
 * History
 * 1.0.2: Updated code to work with server neo4j-1.1
 */
public class App {
	private static final String NEO4J_URL = "http://localhost:7474/db/data/";
	private static final String JSON_FOLDER_URI = "rda/json";
	
	/**
	 * Main class function
	 * @param args String[]
	 */
	public static void main(String[] args) {
		String folderUri = JSON_FOLDER_URI;
		if (args.length > 0 && !args[0].isEmpty())
			folderUri = args[0];
		
		String neo4jUrl = NEO4J_URL;
		if (args.length > 1 && !args[1].isEmpty())
			neo4jUrl = args[1];
		
		Importer importer = new Importer(folderUri, neo4jUrl);
		importer.importRecords();
	}

}
