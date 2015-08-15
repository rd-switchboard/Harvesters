package org.rdswitchboard.tests.crosswalk;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;

public class App {
	private static final String PROPERTIES_FILE = "properties/test_crosswalk.properties";
	private static final String OUT_FILE_NAME = "result.xml";

	public static void main(String[] args) {
		try {
			String propertiesFile = PROPERTIES_FILE;
			
			if (args.length != 0 && !StringUtils.isNullOrEmpty(args[0]))
				propertiesFile = args[0];
			
            Properties properties = new Properties();
	        try (InputStream in = new FileInputStream(propertiesFile)) {
	            properties.load(in);
	        }
	        
	        String accessKey = properties.getProperty("aws.access.key");
	        String secretKey = properties.getProperty("aws.secret.key");
   
	        String bucket = properties.getProperty("s3.bucket");
	        
	        if (StringUtils.isNullOrEmpty(bucket))
                throw new IllegalArgumentException("AWS S3 Bucket can not be empty");

	        System.out.println("S3 Bucket: " + bucket);
	        
	        String key = properties.getProperty("s3.key");
	        	
	        if (StringUtils.isNullOrEmpty(key))
	            throw new IllegalArgumentException("AWS S3 Key can not be empty");
        
	        System.out.println("S3 Key: " + key);

	        String crosswalk = properties.getProperty("crosswalk");
	        if (StringUtils.isNullOrEmpty(crosswalk))
	            throw new IllegalArgumentException("Crosswalk can not be empty");
        
	        System.out.println("Crosswalk: " + crosswalk);
	        
	        String outFileName = properties.getProperty("out", OUT_FILE_NAME);
	        System.out.println("Out: " + outFileName);
	        
	        AmazonS3 s3client;
	        if (!StringUtils.isNullOrEmpty(accessKey) && !StringUtils.isNullOrEmpty(secretKey)) {
	        	System.out.println("Connecting to AWS via Access and Secret Keys. This is not safe practice, consider to use IAM Role instead.");
		        
	        	AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
		        s3client = new AmazonS3Client(awsCredentials);
	        } else {
	        	System.out.println("Connecting to AWS via Instance Profile Credentials");
	        	
	        	s3client = new AmazonS3Client(new InstanceProfileCredentialsProvider());
	        }	     

	        S3Object object = s3client.getObject(new GetObjectRequest(bucket, key));
	        
	        Templates template = TransformerFactory.newInstance().newTemplates(
	        		new StreamSource(
	        					new FileInputStream(crosswalk)));
	        StreamSource reader = new StreamSource(object.getObjectContent());
			StreamResult result = (StringUtils.isNullOrEmpty(outFileName) || outFileName.equals("stdout")) 
					? new StreamResult(System.out)
					: new StreamResult(new FileOutputStream(outFileName));
						
			
			Transformer transformer = template.newTransformer(); 
			transformer.transform(reader, result);
			
			
			/*
			     DocumentBuilderFactory dFactory = DocumentBuilderFactory.newInstance();
	        TransformerFactory tFactory = TransformerFactory.newInstance();
	        XPath xPath = XPathFactory.newInstance().newXPath();
	        
	        DocumentBuilder builder = dFactory.newDocumentBuilder();
			Document document = builder.parse(object.getObjectContent());
			Transformer transformer1 = tFactory.newTemplates(
	        		new StreamSource(new FileInputStream(crosswalk))).newTransformer(); 
			Transformer transformer2 = tFactory.newTransformer();
			
			NodeList metadata = (NodeList)xPath.evaluate("/OAI-PMH/ListRecords/record/metadata",
					document.getDocumentElement(), XPathConstants.NODESET);

			for (int i = 0; i < metadata.getLength(); ++i) {
				System.out.println("Converting node: " + i);
				
			    Element e = (Element) metadata.item(i);
			    Node mets = e.getElementsByTagName("mets").item(0);
			    Node rifcs = document.createElement("registryObjects");
			    
				DOMSource xmlSource = new DOMSource(mets);
			    DOMResult xmlResult = new DOMResult(rifcs);
			    
			    transformer1.transform(xmlSource, xmlResult);
	
			    e.removeChild(mets);
			    e.appendChild(xmlResult.getNode());
			    
			//    e.replaceChild(rifcs, xmlResult.getNode());			    
			}
						
			StreamResult result = (StringUtils.isNullOrEmpty(outFileName) || outFileName.equals("stdout")) 
					? new StreamResult(System.out)
					: new StreamResult(new FileOutputStream(outFileName));
						
			
			transformer2.transform(new DOMSource(document), result);
	
			 
			 */
			
		} catch (Exception e) {
            e.printStackTrace();
		}   
	}

}
