package org.rdswitchboard.importers.ands.s3.ddb;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import org.rdswitchboard.crosswalks.rifcs.graph.Crosswalk;
import org.rdswitchboard.importers.graph.ddb.Importer;
import org.rdswitchboard.libraries.record.Record;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringUtils;

public class App {
	private static final String PROPERTIES_FILE = "properties/import_ands.properties";
	private static final String SOURCE_ANDS = "ands";
	
	public static void main(String[] args) {
		try {
            String propertiesFile = PROPERTIES_FILE;
            if (args.length > 0 && !StringUtils.isNullOrEmpty(args[0])) 
                    propertiesFile = args[0];

            Properties properties = new Properties();
	        try (InputStream in = new FileInputStream(propertiesFile)) {
	            properties.load(in);
	        }
	        
	        String source = properties.getProperty("source");
	        
	        if (StringUtils.isNullOrEmpty(source))
                throw new IllegalArgumentException("Source can not be empty");

	        System.out.println("Source: " + source);
	        
	        int maxThreads = Integer.parseInt(properties.getProperty("max.threads", "0"));
	        if (maxThreads > 1)
	        	System.out.println("Processing with " + maxThreads + " threads");
	        else
	        	System.out.println("Processing with single threads");
	        	        
	        String accessKey = properties.getProperty("aws.access.key");
	        String secretKey = properties.getProperty("aws.secret.key");

	        if (StringUtils.isNullOrEmpty(accessKey) || StringUtils.isNullOrEmpty(secretKey))
                throw new IllegalArgumentException("AWS Access Key and Secret Key can not be empty");
	        	      
	        
	        String bucket = properties.getProperty("s3.bucket");
	        
	        if (StringUtils.isNullOrEmpty(bucket))
                throw new IllegalArgumentException("AWS S3 Bucket can not be empty");

	        System.out.println("S3 Bucket: " + bucket);
	        
	        String prefix = properties.getProperty("s3.prefix");
	        	
	        if (StringUtils.isNullOrEmpty(prefix))
	            throw new IllegalArgumentException("AWS S3 Prefix can not be empty");
        
	        System.out.println("S3 Prefix: " + prefix);
	        
	       /*debugFile(accessKey, secretKey, bucket, "rda/rif/class:collection/54800.xml");*/ 
	        
	        if (maxThreads <= 1)
	        	processSingleThread(accessKey, secretKey, bucket, prefix);
	        else
	        	processMultiThread(accessKey, secretKey, bucket, prefix, maxThreads);
		} catch (Exception e) {
            e.printStackTrace();
		}       
	}
	
	/*
	private static void debugFile(String accessKey, String secretKey, String bucket, String file) throws Exception {
		AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3client = new AmazonS3Client(awsCredentials);
        
        Crosswalk crosswalk = new Crosswalk();
        crosswalk.setVerbose(true);
    	Importer importer = new Importer(awsCredentials);
    	importer.setVerbose(true);

    	System.out.println("Processing file: " + file);
				
		S3Object object = s3client.getObject(new GetObjectRequest(bucket, file));
		InputStream xml = object.getObjectContent();
								
		System.out.println("Parsing file: " + file);
		Collection<Record> records = crosswalk.process(xml).values();

		System.out.println("Uploading " + records.size() + " records");
		importer.importRecords(SOURCE_ANDS, records);
	}
	*/
	
	private static void processSingleThread(String accessKey, String secretKey, String bucket, String prefix) throws Exception {
		AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3client = new AmazonS3Client(awsCredentials);
        
        Crosswalk crosswalk = new Crosswalk();
    	Importer importer = new Importer(awsCredentials);
    
    	ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
			.withBucketName(bucket)
			.withPrefix(prefix);
		ObjectListing objectListing;
		S3Object object;

		do {
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : 
				objectListing.getObjectSummaries()) {
				
				String file = objectSummary.getKey();

		        System.out.println("Processing file: " + file);
				
				object = s3client.getObject(new GetObjectRequest(bucket, file));
				InputStream xml = object.getObjectContent();
								
				System.out.println("Parsing file: " + file);
				Collection<Record> records = crosswalk.process(xml).values();

				System.out.println("Uploading " + records.size() + " records");
				importer.importRecords(SOURCE_ANDS, records);
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());
	}
	
	private static void processMultiThread(String accessKey, String secretKey, 
			String bucket, String prefix, int maxThreads) throws Exception {
		AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3client = new AmazonS3Client(awsCredentials);

		Semaphore semaphore = new Semaphore(maxThreads);

		List<ImportThread> threads = new ArrayList<ImportThread>();
		for (int i = 0; i < maxThreads; ++i) {
			ImportThread thread = new ImportThread(SOURCE_ANDS, semaphore, awsCredentials);
			thread.start();
			threads.add(thread);
		}		
        
    	ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
			.withBucketName(bucket)
			.withPrefix(prefix);
		ObjectListing objectListing;
		S3Object object;	

		do {
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : 
				objectListing.getObjectSummaries()) {
				
				semaphore.acquire(); 

				String file = objectSummary.getKey();
		        System.out.println("Processing file: " + file);
				
				object = s3client.getObject(new GetObjectRequest(bucket, file));
				InputStream xml = object.getObjectContent();
				
				boolean importAssigned = false;
				for (ImportThread thread : threads) 
					if (thread.isFree()) {
						thread.process(xml);
						importAssigned = true;
						
						break;
					}								
				
				if (!importAssigned)
					throw new ImportThreadException("All matcher threads are busy");
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());
		
		for (ImportThread thread : threads) {
			thread.finishCurrentAndExit();
			thread.join();
		}
	}
}