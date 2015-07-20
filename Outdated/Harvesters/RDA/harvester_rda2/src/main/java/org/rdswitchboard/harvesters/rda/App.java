package org.rdswitchboard.harvesters.rda;

/**
 * RDA JSON Harvester
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.0.0
 */
public class App {
	private static final String JSON_FOLDER_URI = "rda/json";

	public static void main(String[] args) {
		
		String folderJson = JSON_FOLDER_URI;
		if (args.length > 0 && !args[0].isEmpty())
			folderJson = args[0];

		// Create harvester object
		Harvester harvester = new Harvester(folderJson);
		
		try {
			// harvest
			harvester.harvest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
