package org.rdswitchboard.nexus.exporters.dryad.json;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.parboiled.common.StringUtils;
import org.rdswitchboard.nexus.exporters.graph.Exporter;
import org.rdswitchboard.utils.aggrigation.AggrigationUtils;

public class App {
	private static void loadProperties(Properties properties, String propertiesFile) throws IOException {
		try (InputStream in = new FileInputStream(propertiesFile)) {
			properties.load(in);
		}
	}
	
	public static void main(String[] args) {
		try {
			Properties properties = new Properties();
			loadProperties(properties, "properties/neo4j_local.properties");
			loadProperties(properties, "properties/aws.properties");
			loadProperties(properties, "properties/export_dryad_json.properties");
			
			String sourceNeo4jFolder = properties.getProperty("neo4j", "neo4j");
			
			String s3AccessKey = properties.getProperty("aws.access.key");
			String s3SecetKey = properties.getProperty("aws.secret.key");
			String s3Bucket = properties.getProperty("s3.bucket");
			String s3Key = properties.getProperty("s3.key");
			boolean s3Public = Boolean.parseBoolean(properties.getProperty("s3.public"));
			
			int maxLevel = Integer.parseInt(properties.getProperty("max.level", "3"));
			int maxNodes = Integer.parseInt(properties.getProperty("max.nodes", "100"));
			int maxSiblings = Integer.parseInt(properties.getProperty("max.siblings", "10"));

			if (StringUtils.isEmpty(s3AccessKey) || StringUtils.isEmpty(s3SecetKey)) {
				System.out.println("S3 Access key and Secret key can not be empty");
				
				return;
			}

			if (StringUtils.isEmpty(s3Bucket)) {
				System.out.println("S3 Bucket name can not be empty");
				
				return;
			}

			if (StringUtils.isEmpty(s3Key)) {
				System.out.println("S3 Key prefix can not be empty");
				
				return;
			}
			
	       	Exporter expoter = new DryadExporter();
	       	expoter.setNeo4jFolder(sourceNeo4jFolder);
	       	expoter.addLabel(AggrigationUtils.Labels.Dryad);
	       	expoter.addLabel(AggrigationUtils.Labels.Dataset);
	       	
	       	expoter.setAwsCredentials(s3AccessKey, s3SecetKey);
	       	expoter.setS3Bucket(s3Bucket);
	       	expoter.setS3Key(s3Key);
	       	expoter.enablePublicReadRights(s3Public);
	       	
	       	expoter.setMaxLevel(maxLevel);
	       	expoter.setMaxNodes(maxNodes);
	       	expoter.setMaxSiblings(maxSiblings);
	        
	       	expoter.process();
	        
		} catch (Exception e) {
			e.printStackTrace();
		} 	
	}
}
