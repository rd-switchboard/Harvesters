/**
 * export_rda_json : org.rdswitchboard.nexus.exporters.rda.json
 * 22 Jun 2015
 */
package org.rdswitchboard.exporters.ands.json;

import org.neo4j.graphdb.Node;
import org.rdswitchboard.exporters.graph.Exporter;
import org.rdswitchboard.libraries.graph.GraphUtils;

/**
 * @version 
 * @author Dima Kudriavcev (dmitrij@kudriavcev.info)
 * @date 22 Jun 2015 	
 *
 */
public class RdsExporter extends Exporter {

	private static final String RDS_SITE = "rd-switchboard.net";
	
	/* (non-Javadoc)
	 * @see org.rdswitchboard.nexus.exporters.graph.Exporter#generateName(org.neo4j.graphdb.Node)
	 */
	@Override
	public String generateName(Node node) {
		String key = (String) node.getProperty(GraphUtils.PROPERTY_KEY);
		if (null != key && key.startsWith(RDS_SITE)) {
			
			String[] arr = key.split("/");
			if (arr.length >= 3) 
				return arr[1] + "-" + arr[2];
				
		} 
		
		return null;
	}
}
