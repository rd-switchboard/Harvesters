package org.rd.switchboard.importers.crossref;

public class App {
	private static final String NEO4J_URL = "http://localhost:7474/db/data/";	
//	private static final String NEO4J_URL = "http://localhost:7476/db/data/";	
//	private static final String NEO4J_URL = "http://ec2-54-187-84-58.us-west-2.compute.amazonaws.com:7474/db/data/";
	private static final String CROSSFRE_CACHE_FOLDER = "crossref/cahce";	
	
	public static void main(String[] args) {
		String neo4jUrl = NEO4J_URL;
		if (args.length > 0 && !args[0].isEmpty())
			neo4jUrl = args[0];
		
		String crossrefFolder = CROSSFRE_CACHE_FOLDER;
		if (args.length > 1 && !args[1].isEmpty())
			crossrefFolder = args[1];
			
		Importer importer = new Importer(neo4jUrl, crossrefFolder);			
		importer.process();
	}
}
