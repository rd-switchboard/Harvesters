package org.rdswitchboard.importers.services;

/**
 * Main class
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public abstract class App {
	private static final String INSTITUTIONS_SCV = "data/services.csv";
	private static final String NEO4J_URL = "http://localhost:7476/db/data/";

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
		importer.importServices(institutionCsv);
	}
}
