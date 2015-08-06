package org.rdswitchboard.nexus.exporters.dryad.json;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.neo4j.graphdb.Node;
import org.rdswitchboard.nexus.exporters.graph.Exporter;
import org.rdswitchboard.utils.aggrigation.AggrigationUtils;

public class DryadExporter extends Exporter {

	@Override
	public String generateName(Node node) {
		String key = (String) node.getProperty(AggrigationUtils.PROPERTY_KEY);
		try {
			if (null != key) 
				return URLEncoder.encode(key, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		return null;
	}
}