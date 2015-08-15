package org.rdswitchboard.importers.graph;

import java.io.IOException;

public class App {
	private static final String SOURCE_NEO4J_URL = "http://localhost:7474/db/data/";	
	private static final String OUTPUT_FOLDER = "graph";	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String sourceNeo4jUrl = SOURCE_NEO4J_URL;
		if (args.length > 0 && !args[0].isEmpty())
			sourceNeo4jUrl = args[0];
			
		String outputFolder = OUTPUT_FOLDER;
		if (args.length > 1 && !args[1].isEmpty())
			outputFolder = args[1];
		
		try {
			Importer importer = new Importer(sourceNeo4jUrl, outputFolder);
			importer.process();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
}
