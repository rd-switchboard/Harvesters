package org.rdswitchboard.harvesters.pmh;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import jdk.nashorn.internal.runtime.regexp.joni.Warnings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.StringUtils;

/**	
 * This file is part of RD-Switchboard.
 *
 * RD-Switchboard is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * 
 * OAI:PMH Harvester Library
 * 
 * This Library is designed to harvest any OAI:PMH repository and store the data locally.
 * The data will be stored as a collection of .xml files. The files will be stored as:
 * <p>
 * {@code
 *   <Base Folder>/<Metadata Name>/<Set Name>/<Record Identifier>.xml
 * }
 * <p>
 * The library will also create an index for every set. The index file will be opened and 
 * closed automatically. The index will be used to track record timestamp and updated record
 * file if needed. The set index will be stored as:
 * <p>
 * {@code 
 *   <Base Folder>/<Metadata Name>/<Set Name>.xml
 * }
 * <p>  
 * The library will cope outdated records in the cache before overwriting them. The cached files will 
 * be stored as:
 * <p>
 * {@code 
 *   <Base Folder>/<Metadata Name>/_cache/<Set Name>/<Record Identifier>_<Record Timestamp>.xml
 * }
 * <p>
 * While working, the library will also create temporary status file. The file could be used to track 
 * library work status. It also will be used to resume harvesting process, if it was terminated by
 * any reason. If such behavior is not needed, the status file must be deleted before calling the
 * harvest function. The status file will be stored as: 
 * <p>
 * {@code 
 *    <Base Folder>/<Metadata Name>/status.xml
 * }
 * <p>
 * To use the library, simple construct the Harvester object and then call the harvest function with 
 * desired medadata prefix. The harvest function can be called several times with different metadata
 * prefixes to harvest different metadata. Example:
 * <p><pre>
 * {@code 
 * 	try {
 *      // CERN repository
 *	    String repoUri = "http://cds.cern.ch/oai2d.py/";
 *
 *      // Base folder to store XML data
 *      String folderXml = "cern/xml";
 *  
 *      // Construct Harvester object.
 *      Harvester harvester = new Harvester(repoUri, folderXml);
 *			
 *      // List and display supported meatadata formats
 *	    List<MetadataFormat> formats = harvester.listMetadataFormats();
 *	    System.out.println("Supported metadata formats:");
 *	    for (MetadataFormat format : formats) {
 *	        System.out.println(format.toString());
 *	    }
 *			
 * 		// harvest the metdata with first prefix on the list
 *		harvester.harvest(formats.get(0).getMetadataPrefix));
 *	} catch (Exception e) {
 *	    e.printStackTrace();
 *	}
 * }</pre>
 * 
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.1.1
 */
public class Harvester {
	
	private static final String URL_IDENTIFY = "?verb=Identify";
	private static final String URL_LIST_METADATA_FORMATS = "?verb=ListMetadataFormats";
	private static final String URL_LIST_SETS = "?verb=ListSets";
	private static final String URL_LIST_RECORDS = "?verb=ListRecords&set=%s&metadataPrefix=%s";
	private static final String URL_LIST_DEFAULT_RECORDS = "?verb=ListRecords&metadataPrefix=%s";
	private static final String URL_LIST_RECORDS_RESUMPTION_TOKEN = "?verb=ListRecords&resumptionToken=%s";
	private static final String URL_LIST_SETS_RESUMPTION_TOKEN = "?verb=ListSets&resumptionToken=%s";
	
	private static final String ERR_NO_RECORDS_MATCH = "noRecordsMatch";
	
	private static XPathExpression XPATH_OAI_PMH;
	private static XPathExpression XPATH_ERROR;
	private static XPathExpression XPATH_REPOSITORY_NAME;
	private static XPathExpression XPATH_PROTOCOL_VERSION;
	private static XPathExpression XPATH_EARLEST_TIMESHTAMP;
	private static XPathExpression XPATH_DELETED_RECORD;
	private static XPathExpression XPATH_GRANULARITY;
	private static XPathExpression XPATH_ADMIN_EMAIL;
	private static XPathExpression XPATH_LIST_SETS;
	private static XPathExpression XPATH_SET_NAME;
	private static XPathExpression XPATH_SET_SPEC;
	private static XPathExpression XPATH_RECORDS_RESUMPTION_TOKEN;
	private static XPathExpression XPATH_SETS_RESUMPTION_TOKEN;
	
	private static String harvestDate;
	
	static {
		XPath xPath = XPathFactory.newInstance().newXPath();
		try {
			XPATH_REPOSITORY_NAME = xPath.compile("/OAI-PMH/Identify/repositoryName/text()");
			XPATH_PROTOCOL_VERSION = xPath.compile("/OAI-PMH/Identify/protocolVersion/text()");
			XPATH_EARLEST_TIMESHTAMP = xPath.compile("/OAI-PMH/Identify/earliestTimestamp/text()");
			XPATH_DELETED_RECORD = xPath.compile("/OAI-PMH/Identify/deletedRecord/text()");
			XPATH_GRANULARITY = xPath.compile("/OAI-PMH/Identify/granularity/text()");
			XPATH_ADMIN_EMAIL = xPath.compile("/OAI-PMH/Identify/adminEmail/text()");
			
			XPATH_LIST_SETS = xPath.compile("/OAI-PMH/ListSets/set");
			XPATH_SET_NAME = xPath.compile("./setName/text()");
			XPATH_SET_SPEC = xPath.compile("./setSpec/text()");

			XPATH_OAI_PMH = xPath.compile("/OAI-PMH");
			XPATH_ERROR = xPath.compile("./error");
			XPATH_RECORDS_RESUMPTION_TOKEN = xPath.compile("./ListRecords/resumptionToken");
			XPATH_SETS_RESUMPTION_TOKEN = xPath.compile("/OAI-PMH/ListSets/resumptionToken");
			
			harvestDate = new SimpleDateFormat("yyyy-MM-dd").format(DateTime.now().toDate());
			
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * variable to store repo URL. Can not be null.
	 */
	private String repoUrl;
	
	private String bucketName;
	private String folderName;
	
	private String repoPrefix;
	
	private String metadataPrefix;
	
	private final Map<String, SetStatus> processedSets = new HashMap<String, SetStatus>(); 
	
	private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	
	private String repositoryName;
	private String protocolVersion;
	private String earliestTimestamp;
	private String deletedRecord;
	private String granularity;
	private String adminEmail;
	
	private Set<String> blackList;
	private Set<String> whiteList;
	
	private boolean failOnError;
	private int maxAttempts;
	private int attemptDelay;
	private int connectionTimeout;
	private int readTimeout;
	
	private AmazonS3 s3client;
	

	
	/**
	 * Harvester constructor
	 * 
	 * @param repoUrl : The Repository URL
	 * @param folderBase : The address of the folder, there received data must be saved.
	 * @throws JAXBException 
	 * @throws IOException 
	 * @throws Exception 
	 */
	public Harvester( final Properties properties ) throws Exception {
		repoUrl = properties.getProperty("url");
		if (StringUtils.isNullOrEmpty(repoUrl)) 
			throw new IllegalArgumentException("The OAI:PMH Repository URL can not be empty");

		repoPrefix = properties.getProperty("name");

		if (StringUtils.isNullOrEmpty(repoPrefix))
			throw new IllegalArgumentException("The OAI:PMH Repository Prefix can not be empty");
			
		metadataPrefix = properties.getProperty("metadata");
		
		String accessKey = properties.getProperty("aws.access.key");
		String secretKey = properties.getProperty("aws.secret.key");
		if (StringUtils.isNullOrEmpty(accessKey) || StringUtils.isNullOrEmpty(secretKey)) 
			s3client = new AmazonS3Client(new InstanceProfileCredentialsProvider());
		else
			s3client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey)); 
		
		bucketName = properties.getProperty("s3.bucket");
		folderName = properties.getProperty("folder");
		
		if (StringUtils.isNullOrEmpty(bucketName) && StringUtils.isNullOrEmpty(folderName))
			throw new IllegalArgumentException("Please enter either local folder name or AWS S3 Bucket name to store the harvested files");
		if (!StringUtils.isNullOrEmpty(bucketName) && !StringUtils.isNullOrEmpty(folderName))
			throw new IllegalArgumentException("S3 bucket and local folder parameters can not be used at the same time. Please disable one in the configuration file.");
		
		try {
			File fileBlackList = new File(properties.getProperty("black.list"));
			if (fileBlackList.isFile()) {
				List<String> list = FileUtils.readLines(fileBlackList);
				blackList = new HashSet<String>();
				for (String l : list) {
					String s = l.trim();
					if (!s.isEmpty())
						blackList.add(s); 
				}
			}
		} catch (Exception e) {
			blackList = null;
		}
		
		try {
			File fileWhiteList = new File(properties.getProperty("white.list"));
			if (fileWhiteList.isFile()) {
				List<String> list = FileUtils.readLines(fileWhiteList);
				whiteList = new HashSet<String>();
				for (String l : list) {
					String s = l.trim();
					if (!s.isEmpty())
						whiteList.add(s); 
				}
			}
		} catch (Exception e) {
			whiteList = null;
		}
		
		if (null != blackList && !blackList.isEmpty() && null != whiteList && !whiteList.isEmpty())
			throw new Exception ("The black and the withe list parameters can not be set at the same time. Please disable one in the configuration file."); 
		
		connectionTimeout = Integer.parseInt(properties.getProperty("conn.timeout", "0"));
		readTimeout = Integer.parseInt(properties.getProperty("read.timeout", "0"));
		maxAttempts = Integer.parseInt(properties.getProperty("max.attempts", "0"));
		attemptDelay = Integer.parseInt(properties.getProperty("attempt.delay", "0"));
		failOnError = Boolean.parseBoolean(properties.getProperty("fail.on.error", "true"));
		
	}


	public void setWhiteList(Set<String> set){
        whiteList=new HashSet<String>();
        whiteList.addAll(set);
	}

	/**
	 * Return the repository name (available after calling identify() function).
	 * @return String - Repository name
	 */
	public String getRepositoryName() { return repositoryName; }
	
	/**
	 * Return protocol version (available after calling identify() function).
	 * @return String - protocol version
	 */
	public String getProtocolVersion() { return protocolVersion; }
	
	/**
	 * Return the earliest timestamp in the repository (available after calling identify() function).
	 * @return String - earliest timestamp
	 */
	public String getEarliestTimestamp() { return earliestTimestamp; }
	
	/**
	 * Return deleted record behavior (available after calling identify() function).
	 * @return String - deleted record behavior
	 */
	public String getDeletedRecordBehavior() { return deletedRecord; }
	
	/**
	 * Return granularity template (available after calling identify() function).
	 * @return String - granularity template
	 */
	public String getGranularityTemplate() { return granularity; }
	
	/**
	 * Return admin email of repositiry (available after calling identify() function).
	 * @return String - admin email
	 */
	public String getAdminEmail() { return adminEmail; }	
	
	public String getMetadataPrefix() { return metadataPrefix; }

	
	/**
	 * Function to indentify on OAI:PMH Server. Could be used to test connection with the server.
	 * Also will initialize all server information variables.
	 * @return true if connection could be established and server didn't return any error.
	 */
	public boolean identify() {
		String url =  repoUrl + URL_IDENTIFY;
		
		try {
			Document doc = dbf.newDocumentBuilder().parse(url);
			
			repositoryName = (String) XPATH_REPOSITORY_NAME.evaluate(doc, XPathConstants.STRING);
			protocolVersion = (String) XPATH_PROTOCOL_VERSION.evaluate(doc, XPathConstants.STRING);
			earliestTimestamp = (String) XPATH_EARLEST_TIMESHTAMP.evaluate(doc, XPathConstants.STRING);
			deletedRecord = (String) XPATH_DELETED_RECORD.evaluate(doc, XPathConstants.STRING);
			granularity = (String) XPATH_GRANULARITY.evaluate(doc, XPathConstants.STRING);
			adminEmail = (String) XPATH_ADMIN_EMAIL.evaluate(doc, XPathConstants.STRING);
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	/**
	 * Function to list supported metadata formats
	 * @return {@code List<MetadataFormat>} - list of metadata formats
	 */
	public List<MetadataFormat> listMetadataFormats() {
		String url =  repoUrl + URL_LIST_METADATA_FORMATS;
		
		try {
			Document doc = dbf.newDocumentBuilder().parse(url);
			
			return MetadataFormat.getMetadataFormats(doc);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		return null;
	}
	
	/**
	 * Function to list sets
	 * @return Map<String, String> where Key is set name and Value is set specification
	 */
	public Map<String, String> listSets() {
		String url =  null;
		String tokenString = null;
		try {
			Map<String, String> mapSets = new HashMap<String, String>();

			do {
				if (tokenString==null){
					url =  repoUrl + URL_LIST_SETS;
				}else{
					url = repoUrl + String.format(URL_LIST_SETS_RESUMPTION_TOKEN, URLEncoder.encode(tokenString, "UTF-8"));
				}
				Document doc = dbf.newDocumentBuilder().parse(url);
				NodeList sets = (NodeList) XPATH_LIST_SETS.evaluate(doc, XPathConstants.NODESET);
				for (int i = 0; i < sets.getLength(); i++) {
					Node set = sets.item(i);
					String setName = (String) XPATH_SET_NAME.evaluate(set, XPathConstants.STRING);
					String setGroup = (String) XPATH_SET_SPEC.evaluate(set, XPathConstants.STRING);

					if (mapSets.put(setGroup, setName) != null) {
						System.out.println("Warning, the group already exists in the set: " + setGroup + " | " + setName);
					}
				}

				Node nodeToken = (Node)  XPATH_SETS_RESUMPTION_TOKEN.evaluate(doc, XPathConstants.NODE);
				if (null != nodeToken && nodeToken instanceof Element) {
					 tokenString = ((Element) nodeToken).getTextContent();
				}
			}while(null != tokenString && !tokenString.isEmpty());

			return mapSets;						
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
		return null;
	}
	
	public void downloadRecords( SetStatus set ) throws 
			HarvesterException, UnsupportedEncodingException, IOException, 
			InterruptedException, XPathExpressionException, SAXException, 
			ParserConfigurationException {

		// Generate the URL of request
		String url = null; ;
		if (set.hasToken()) {
			try {
				url = repoUrl + String.format(URL_LIST_RECORDS_RESUMPTION_TOKEN, URLEncoder.encode(set.getToken(), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		if (null == url) {
			if (!set.hasName())
				url = repoUrl + String.format(URL_LIST_DEFAULT_RECORDS, metadataPrefix);
			else
				url = repoUrl + String.format(URL_LIST_RECORDS,  URLEncoder.encode(set.getName(), "UTF-8"), metadataPrefix);
		}
		
		System.out.println("Downloading records: " + url);
		
		String xml = null;
		
		// Get XML document 
		URLConnection conn = new URL(url).openConnection();
		if (connectionTimeout > 0)
			conn.setConnectTimeout(connectionTimeout);
		if (readTimeout > 0)
			conn.setReadTimeout(readTimeout);
		try (InputStream is = conn.getInputStream()) {
	    	if (null != is) 
	    		xml = IOUtils.toString(is, StandardCharsets.UTF_8.name()); 
	    } 			    
	    
	    // Check if xml has been returned and check what it had a valid root element
		if (null == xml) 
			throw new HarvesterException("The XML document is empty");
        		Document doc = dbf.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));

		// Extract root node
		Node root = (Node) XPATH_OAI_PMH.evaluate(doc, XPathConstants.NODE);
		if (null == root)
			throw new HarvesterException("The document is not an OAI:PMH file");
	
		// Check for error node
		Node error = (Node) XPATH_ERROR.evaluate(root, XPathConstants.NODE); 
		if (null != error && error instanceof Element) {
			String code = ((Element) error).getAttribute("code");
			String message = ((Element) error).getTextContent();
			
			if (ERR_NO_RECORDS_MATCH.equals(code))
			{
				System.out.println("Error: The set is empty");

				set.setFiles(0);
				set.resetToken();
				
				return;
			} else 
				throw new HarvesterException (code, message);
		}
				
		Node nodeToken = (Node) XPATH_RECORDS_RESUMPTION_TOKEN.evaluate(root, XPathConstants.NODE);
				
		if (null != nodeToken && nodeToken instanceof Element) {
			String tokenString = ((Element) nodeToken).getTextContent();
			if (null != tokenString && !tokenString.isEmpty())
				set.setToken(tokenString);
			else
				set.resetToken();
			
			set.setCursor(((Element) nodeToken).getAttribute("cursor"));
			set.setSize(((Element) nodeToken).getAttribute("completeListSize"));
			
			set.dumpToken(System.out);
		} else
			set.resetToken();
		
		
		String filePath = repoPrefix + "/" + metadataPrefix + "/" + harvestDate + "/" + set.getNameSafe() + "/" + set.getFiles() + ".xml";
		
		if (StringUtils.isNullOrEmpty(bucketName)) {
			
			FileUtils.writeStringToFile(new File(folderName, filePath), xml);
			
		} else {
			byte[] bytes = xml.getBytes(StandardCharsets.UTF_8);
			
			ObjectMetadata metadata = new ObjectMetadata();
	        metadata.setContentEncoding(StandardCharsets.UTF_8.name());
	        metadata.setContentType("text/xml");
	        metadata.setContentLength(bytes.length);
	
	        InputStream inputStream = new ByteArrayInputStream(bytes);
	
	        PutObjectRequest request = new PutObjectRequest(bucketName, filePath, inputStream, metadata);
	
	        s3client.putObject(request);
		}
		
        set.incFiles();
	}
	

	
	/**
	 * Main function to organize the standard harvest process. The function will identify 
	 * on the server and will download list of the sets. Then it will download each set, 
	 * creating index, if needed. Every record of the set will be stored in the separate 
	 * file in the set folder. If function will find, that some record has been changed, 
	 * the old record will be stored in the set cache folder. The function will also create 
	 * and update harvesting status in the xml file under repo base folder. This file will 
	 * be deleted after harvesting process will be finished. If, by any reason, harvesting 
	 * process will be terminated, the status file will be left on disk. In this case the 
	 * function will try to load in and resume it work. If, by some reason this behaviour 
	 * is not required, the status file must be deleted before calling this function.
	 * @param prefix A metadata prefix
	 * @throws Exception
	 */

	/**
	 * Alternative function to organize the harvest process. The difference with another function
	 * is in data storage. The harvest2 function will store files in the raw format as they come
	 * from the server.
	 * The harvesting method should never be mixed. The harvesting folder must be wiped out if 
	 * switching to this method, or function will fail.
	 * @param prefix A metadata prefix
	 * @throws Exception
	 */
	public boolean harvest() throws Exception {
		if (StringUtils.isNullOrEmpty(metadataPrefix))
			throw new IllegalArgumentException("The OAI:PMH Metadata Prefix can not be empty");

		System.out.println("Downloading set list");

        boolean result=false;

        if (null == whiteList || whiteList.isEmpty()) {


            System.out.println("There is no whitelist found. Proceeding with downloading the list of all available sets.");

            // download all sets in the repository
            Map<String, String> mapSets = listSets();

            if (null == mapSets || mapSets.isEmpty()) {
                System.out.println("Processing default set");

                result = harvestSet(new SetStatus(null, "Default"));
            } else {

                result = false;

                for (Map.Entry<String, String> entry : mapSets.entrySet()) {

                    SetStatus set = new SetStatus(entry.getKey().trim(), URLDecoder.decode(entry.getValue(), StandardCharsets.UTF_8.name()));

                    // if black list exists and item is blacklisted, continue
                    if (null != blackList && blackList.contains(set)) {
                        set.setFiles(-2);
                        saveSetStats(set); // set was ignored
                        continue;
                    }

                    System.out.println("Processing set: " + URLDecoder.decode(entry.getValue(), StandardCharsets.UTF_8.name()));

                    if (!harvestSet(set)) {
                        System.err.println("The harvesting job has been aborted due to an error. If you want harvesting to be continued, please set option 'fail.on.error' to 'false' in the configuration file");
                        result = false;
                        break;
                    } else
                        result = true;
                }

            }
        }else{
            for (String item:whiteList
                 ) {
                if (!harvestSet(new SetStatus(item,item))) {
                    System.err.println("The harvesting job has been aborted due to an error. If you want harvesting to be continued, please set option 'fail.on.error' to 'false' in the configuration file");
                    result = false;
                    break;
                } else
                    result = true;
            }

        }
		if (result)
		{
			String filePath = repoPrefix + "/" + metadataPrefix + "/latest.txt";
			
			if (StringUtils.isNullOrEmpty(bucketName)) {
				
				FileUtils.writeStringToFile(new File(folderName, filePath), harvestDate);
			
			} else {				
				
				byte[] bytes = harvestDate.getBytes(StandardCharsets.UTF_8);
				
				ObjectMetadata metadata = new ObjectMetadata();
		        metadata.setContentEncoding(StandardCharsets.UTF_8.name());
		        metadata.setContentType("text/plain");
		        metadata.setContentLength(bytes.length);
		
		        InputStream inputStream = new ByteArrayInputStream(bytes);
		
		        PutObjectRequest request = new PutObjectRequest(bucketName, filePath, inputStream, metadata);
		
		        s3client.putObject(request);
			}
		}
		
        return result;
	}
	
	private boolean harvestSet(SetStatus set) throws Exception {
		long mark = System.currentTimeMillis();
		
		do {
			for (int nAttempt = 0; nAttempt <= maxAttempts; ++nAttempt)
				try {
					downloadRecords(set);
					
					break;
				} catch (Exception e) {
					// only for debug!
					e.printStackTrace();
					
					if (nAttempt == maxAttempts) {
						
						System.err.println("Error: " + e.getMessage());
						
						set.setError(e.getMessage());
						set.resetToken();
						
						break;
					}
					
					Thread.sleep(attemptDelay);
				}
		} while (set.hasToken());		

		set.setMilliseconds(System.currentTimeMillis() - mark);
		saveSetStats(set);

		if (failOnError && set.hasError())
			return false; 
		
		return true;
	}
	
	public void printStatistics(boolean result, PrintStream out) {
		out.println();
		if (result)
			out.println("The harvesting process has been finished successfully");
		else
			out.println("The harvesting process has been completed with some errors");
		int errorSets = 0;
		int harvestedSets = 0;
		int emptySets = 0;
		int ignoredSets = 0;
		int blacklistedSets = 0;
		
		for (SetStatus set : processedSets.values()) {
			if (set.getFiles() == 0)
				++emptySets;
			else if (set.getFiles() > 0)
				++harvestedSets;
			else if (set.getFiles() == -1)
				++ignoredSets;
			else if (set.getFiles() == -2)
				++blacklistedSets;
			else if (set.getFiles() == -3)
				++errorSets;
		}
		
		if (harvestedSets > 0)
		{
			out.println();
			out.println(String.format("Successfully harvested %s %s:", harvestedSets, harvestedSets == 1 ? "set" : "sets"));
			int counter = 1;
			for (SetStatus set : processedSets.values()) 
				if (set.getFiles() > 0)
					out.println(String.format("Set %d. %s (%s): %d %s has been harvested in %s", 
							counter++, set.getTitle(), set.getName(), set.getFiles(), set.getFiles() == 1 ? "file" : "files", set.getEllapsedTime()));
		}

		if (emptySets > 0)
		{
			out.println();
			out.println(String.format("%d %s has been empty:", emptySets, emptySets == 1 ? "set" : "sets"));
			int counter = 1;
			for (SetStatus set : processedSets.values()) 
				if (set.getFiles() == 0)
					out.println(String.format("Set %d. %s (%s)", 
							counter++, set.getTitle(), set.getName()));		
			}
		
		if (ignoredSets > 0)
		{
			out.println();
			out.println(String.format("%d %s has been ignored by the WHITE list:", ignoredSets, ignoredSets == 1 ? "set" : "sets"));
			int counter = 1;
			for (SetStatus set : processedSets.values()) 
				if (set.getFiles() == -1)
					out.println(String.format("Set %d. %s (%s)", 
							counter++, set.getTitle(), set.getName()));
		}

		if (blacklistedSets > 0)
		{
			out.println();
			out.println(String.format("%d %s has been ignored by the black list:", blacklistedSets, blacklistedSets == 1 ? "set" : "sets"));
			int counter = 1;
			for (SetStatus set : processedSets.values()) 
				if (set.getFiles() == -2)
					out.println(String.format("Set %d. %s (%s)", 
							counter++, set.getTitle(), set.getName()));
		}
		
		if (errorSets > 0)
		{
			out.println();
			out.println(String.format("%d %s has got an error:", errorSets, errorSets == 1 ? "set" : "sets"));
			int counter = 1;
			for (SetStatus set : processedSets.values()) 
				if (set.getFiles() == -3)
					out.println(String.format("Set %d. %s (%s) error: %s", 
							counter++, set.getTitle(), set.getName(), set.getError()));
		}
	}
	
	private void saveSetStats(SetStatus set) {
		processedSets.put(set.getNameSafe(), set);
	}
	
	/**
	 * Function to get Repo URL
	 * @return String - Repo URL
	 */
	public String getRepoUrl() {
		return repoUrl;
	}

	/**
	 * Function to set Repo URL
	 * @param repoUrl an Repo URL
	 */
	public void setRepoUrl(String repoUrl) {
		this.repoUrl = repoUrl;
	}
}
