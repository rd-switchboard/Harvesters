package org.rd.switchboard.importers.nhmrc;

/**
 * HISTORY:
 * 1.0.1: Switched import to server neo4j-1.1
 * 
 * @author dima
 *
 */

public class App {

	//private static final String NEO4J_URL = "http://ec2-54-69-203-235.us-west-2.compute.amazonaws.com:7474/db/data/"; 
	//private static final String NEO4J_URL = "http://localhost:7474/db/data/";
	private static final String NEO4J_FOLDER = "neo4j-1";

	/**
	 * Main class function
	 * @param args String[] Neo4J URL.
	 * If missing, the default parameters will be used.
	 */
	public static void main(String[] args) {
		String neo4jFolder = NEO4J_FOLDER;
		if (args.length > 1 && null != args[1] && !args[1].isEmpty())
			neo4jFolder = args[1];
		
		Importer importer = new Importer(neo4jFolder);
		importer.importGrants();
	}
}
