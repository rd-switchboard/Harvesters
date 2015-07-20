package org.rdswitchboard.importers.institutions;

/**
 * Main class for Web:Institution importer
 * 
 * This software design to process institutions.csv file located in the working directory
 * and will post data into Neo4J located at http://localhost:7474/db/data/
 * <p>
 * The institutions.csv fomat should be:
 * <br>
 * country,state,institution_name,institution_url
 * <p>
 * The first file line will be counted as header and will be ignored. The Institution host will be 
 * automatically extracted from an institution url
 * 
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.0.0
 */


public class App {
	public static final String INSTITUTIONS_SCV = "data/institutions.csv";
	//private static final String NEO4J_URL = "http://ec2-54-69-203-235.us-west-2.compute.amazonaws.com:7476/db/data/"; 
	private static final String NEO4J_URL = "http://localhost:7474/db/data/";

	/**
	 * Main class function
	 * @param args String[] Expected to have path to the institutions.csv file and Neo4J URL.
	 * If missing, the default parameters will be used.
	 */
	public static void main(String[] args) {
		String institutionCsv = INSTITUTIONS_SCV;
		if (args.length > 0 && null != args[0] && !args[0].isEmpty())
			institutionCsv = args[0];
		
		String neo4jUrl = NEO4J_URL;
		if (args.length > 1 && null != args[1] && !args[1].isEmpty())
			neo4jUrl = args[1];
		
		Importer importer = new Importer(neo4jUrl);
		importer.importInstitutions(institutionCsv);
	}

	// avid-involution-736
}
