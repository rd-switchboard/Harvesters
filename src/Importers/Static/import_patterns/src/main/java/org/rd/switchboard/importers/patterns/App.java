package org.rd.switchboard.importers.patterns;

public class App {
	private static final String PATTERNS_SCV = "data/patterns.csv";
//	private static final String NEO4J_URL = "http://ec2-54-69-203-235.us-west-2.compute.amazonaws.com:7476/db/data/"; 
//	private static final String NEO4J_URL = "http://localhost:7474/db/data/";
	private static final String NEO4J_URL = "http://localhost:7474/db/data/";

	/**
	 * Main class function
	 * @param args String[] Expected to have path to the institutions.csv file and Neo4J URL.
	 * If missing, the default parameters will be used.
	 */
	public static void main(String[] args) {
		String patternsCsv = PATTERNS_SCV;
		if (args.length > 0 && null != args[0] && !args[0].isEmpty())
			patternsCsv = args[0];
		
		String neo4jUrl = NEO4J_URL;
		if (args.length > 1 && null != args[1] && !args[1].isEmpty())
			neo4jUrl = args[1];
		
		Importer importer = new Importer(neo4jUrl);
		importer.importPatterns(patternsCsv);
	}
}
