package org.rdswitchboard.importers.browser.s3;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Properties;

import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;

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
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class App {
	private static final String PROPERTIES_FILE = "properties/import_ands.properties";
	private static final String SOURCE_ANDS = "ands";
	private static final String ENCODING = "UTF-8";
	
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
	  
	        String baseUrl = properties.getProperty("base.url");
	        
	        if (StringUtils.isNullOrEmpty(baseUrl))
                throw new IllegalArgumentException("Base URL can not be empty");

	        System.out.println("Base URL: " + baseUrl);

	        String sessionId = properties.getProperty("session.id");
	        
	        if (StringUtils.isNullOrEmpty(sessionId))
                throw new IllegalArgumentException("Session Id can not be empty");

	        System.out.println("Session Id: " + sessionId);
	        
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
	        
	        Client client = Client.create();
	        Cookie cookie = new Cookie("PHPSESSID", properties.getProperty("session"));
	        
	        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
	        AmazonS3 s3client = new AmazonS3Client(awsCredentials);
	     
	        
	        //String file = "rda/rif/class:collection/54800.xml";
	        
	        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(bucket)
				.withPrefix(prefix);
			ObjectListing objectListing;
			do {
				objectListing = s3client.listObjects(listObjectsRequest);
				for (S3ObjectSummary objectSummary : 
					objectListing.getObjectSummaries()) {
					
					String file = objectSummary.getKey();
	
					System.out.println("Processing file: " + file);
					
					S3Object object = s3client.getObject(new GetObjectRequest(bucket, file));
					InputStream is = object.getObjectContent();
					
					String xml = IOUtils.toString(is, ENCODING); 
					
			        URL url = new URL(baseUrl + "/import/import_s3/");

			        StringBuilder sb = new StringBuilder();
			        addParam(sb, "id", source);
			        addParam(sb, "xml", xml);
			        
					//System.out.println(sb.toString());
			        
			        WebResource webResource = client.resource(url.toString());
					ClientResponse response = webResource
							 	.header("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0")
							 	.accept( MediaType.APPLICATION_JSON, "*/*" )
							 	.acceptLanguage( "en-US", "en" )
							 	.type( MediaType.APPLICATION_FORM_URLENCODED )
		                        .cookie(cookie)
					 			.post(ClientResponse.class, sb.toString());
					 
					if (response.getStatus() != 200) {
						throw new RuntimeException("Failed : HTTP error code : "
								+ response.getStatus());
					}

					String output = response.getEntity(String.class);
				 
					System.out.println(output);
				}
				listObjectsRequest.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());
	      
	        
	        
		} catch (Exception e) {
            e.printStackTrace();
		}       
	}
	
	public static void addParam(StringBuilder sb, String param, String value) throws UnsupportedEncodingException {
		sb.append("&");
		sb.append(URLEncoder.encode(param, ENCODING));
		sb.append("=");
		sb.append(URLEncoder.encode(value, ENCODING));
	}
}