package org.rdswitchboard.harvesters.pmh.s3;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.StringUtils;


/**
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
	
	//private static final String ELEMENT_ROOT = "OAI-PMH";
	
	private static final String REG_RESUMPTION_TOKEN = "<.*resumptionToken.*>.*<.*/.*resumptionToken.*>";
	private static final String REG_RESUMPTION_TOKEN_START = "<.*resumptionToken.*>";
	private static final String REG_RESUMPTION_TOKEN_END = "<.*/.*resumptionToken.*>";
//	private static final String REG_RESUMPTION_TOKEN_SIZE = "completeListSize.*=";
//	private static final String REG_RESUMPTION_TOKEN_CURSOR = "cursor.*=";
//	private static final String REG_RESUMPTION_TOKEN_VALUE = "\".*\"";
	
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
	private static XPathExpression XPATH_RESUMPTION_TOKEN; 
	
	private static final Pattern PATTERN_RESUMPTUON_TOKEN = Pattern.compile(REG_RESUMPTION_TOKEN);
	private static final Pattern PATTERN_RESUMPTUON_TOKEN_START = Pattern.compile(REG_RESUMPTION_TOKEN_START);
	private static final Pattern PATTERN_RESUMPTUON_TOKEN_END = Pattern.compile(REG_RESUMPTION_TOKEN_END);
//	private static final Pattern PATTERN_RESUMPTUON_TOKEN_SIZE = Pattern.compile(REG_RESUMPTION_TOKEN_SIZE);
//	private static final Pattern PATTERN_RESUMPTUON_TOKEN_CURSOR = Pattern.compile(REG_RESUMPTION_TOKEN_CURSOR);
//	private static final Pattern PATTERN_RESUMPTUON_TOKEN_VALUE = Pattern.compile(REG_RESUMPTION_TOKEN_VALUE);
	
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
			XPATH_RESUMPTION_TOKEN = xPath.compile("./ListRecords/resumptionToken");
			
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
	
	private String repoPrefix;
	
	private String metadataPrefix;
	
	
	
	/**
	 * Variable to store base folder for harvested data. Can not be null.
	 */
	//protected String folderBase;
	
	/**
	 * Variable to store folder for current meta data. Will be automatically 
	 * initialized after harvesting process will be started.
	 */
	//protected String folderXml;

	/**
	 * Variable to store current index name as {@code<folderXml>/<setName>.idx} .
	 */
	//protected String indexName;
	
	/**
	 * File encoding (UTF-8 is default)
	 */
	//protected String encoding;

	/**
	 * Variable to store current set index
	 */
	//protected Map<String, Record> records;
	
	/**
	 * Document builder factory to load and parse XML documents
	 */
	private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	
	
	/**
	 * xPath processor
	 */
	//protected XPath xPath = XPathFactory.newInstance().newXPath();
	
//	private String responseDate;
	private String repositoryName;
//	private String baseUrl; // no need base Url, as we have repoUrl instead
	private String protocolVersion;
	private String earliestTimestamp;
	private String deletedRecord;
	private String granularity;
	private String adminEmail;
	
	private Set<String> blackList;
	private Set<String> whiteList;
	
	private int setSize;
	private int setOffset;
	
	private AmazonS3 s3client;
	
/*	private JAXBContext jaxbContext;
	private Marshaller jaxbMarshaller;
	private Unmarshaller jaxbUnmarshaller;*/
	
//	private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
//	private TransformerFactory transformerFactory = TransformerFactory.newInstance();
	/**
	 * Harvester constructor
	 * 
	 * @param repoUrl : The Repository URL
	 * @param folderBase : The address of the folder, there received data must be saved.
	 * @throws JAXBException 
	 * @throws IOException 
	 * @throws Exception 
	 */
	public Harvester( final Properties properties ) throws IOException {
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
		if (StringUtils.isNullOrEmpty(bucketName))
			throw new IllegalArgumentException("The AWS S3 Bucket name can not be empty");
	
/*		jaxbContext = JAXBContext.newInstance(Status.class);
		jaxbMarshaller = jaxbContext.createMarshaller();
		jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		jaxbUnmarshaller = jaxbContext.createUnmarshaller();*/
		
		try {
			File fileBlackList = new File(properties.getProperty("black.list"));
			if (fileBlackList.isFile())
				blackList = new HashSet<String>(FileUtils.readLines(fileBlackList));
		} catch (Exception e) {
			blackList = null;
		}
		
		try {
			File fileWhiteList = new File(properties.getProperty("white.list"));
			if (fileWhiteList.isFile())
				whiteList = new HashSet<String>(FileUtils.readLines(fileWhiteList));
		} catch (Exception e) {
			whiteList = null;
		}
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
	 * Return encoding (if null, the default UTF-8 encoding will be used)
	 * @return String - encoding
	 */
	/*public String getEncoding() {
		return encoding;
	}*/

	/**
	 * Set encoding (UTF-8 is by default)
	 * @param encoding String
	 */
	/*public void setEncoding(String encoding) {
		this.encoding = encoding;
	}*/
	
	/**
	 * Protected function to return XML Document
	 * @param uri resource URI
	 * @return Document - the loaded document
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	/*protected Document GetXml( final String uri ) throws ParserConfigurationException, SAXException, IOException {
		dbf.setNamespaceAware(true);
	    dbf.setExpandEntityReferences(false);
	    dbf.setXIncludeAware(true);
	    
	  //  dbf.setValidating(dtdValidate || xsdValidate);
		
	    InputStream is = new URL(uri).openStream();
	    InputSource xml = new InputSource(is);
	    if (null != encoding)
	    	xml.setEncoding(encoding);
	    
		DocumentBuilder db = dbf.newDocumentBuilder(); 
		Document doc = db.parse(xml);
		
		return doc;
	}*/

	/*
	protected String getString( final String url ) {	
		ClientResponse response = Client.create()
								  .resource( url )
								  .get( ClientResponse.class );
		
		if (response.getStatus() == 200) 
			return response.getEntity( String.class );
		
		return null;
    } */
	
	/**
	 * Protected function to print Document
	 * @param doc XML Document
	 * @param out Output Stream (System.out to print in console)
	 * @throws IOException
	 * @throws TransformerException
	 */
	/*protected void printDocument(Document doc, OutputStream out) 
			throws IOException, TransformerException {
	    Transformer transformer = transformerFactory.newTransformer();
	    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
	    transformer.setOutputProperty(OutputKeys.METHOD, "xml");
	    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
	    transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
	    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
	
	    transformer.transform(new DOMSource(doc), 
	         new StreamResult(new OutputStreamWriter(out, "UTF-8")));
	}*/
	
	/**
	 * Protected function to find child element by tag name 
	 * @param parent Parent element
	 * @param tagName Tag name
	 * @return Element - Child element or null if such element could not be found
	 */
	/*protected Element getChildElementByTagName(Element parent, String tagName) {
	    for(Node child = parent.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element && tagName.equals(((Element) child).getTagName())) 
	        	return (Element) child;
	    return null;
	}*/
	
	/**
	 * Protected function to find child elements by tag name 
	 * @param parent Parent element
	 * @param tagName Tag name
	 * @return {@code List<Element>} - List of child elements or null if such element could not be found
	 */
	/*protected List<Element> getChildElementsByTagName(Element parent, String tagName) {
		List<Element> elements = null;
		for(Node child = parent.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element && tagName.equals(((Element) child).getTagName())) {
	        	if (null == elements)
	        		elements = new ArrayList<Element>();
	        	elements.add((Element) child);
	        }
	    return elements;
	}*/
	
	/**
	 * Protected function to return Document element by tag name
	 * @param doc - XML Document
	 * @param tagName - Tag name
	 * @return Element - Element or null if such element could not be found
	 */
	/*protected Element getDocumentElementByTagName(Document doc, String tagName) {
		NodeList list = doc.getElementsByTagName(tagName);
		if (null != list)
			for (int i = 0; i < list.getLength(); ++i) {
				Node node = list.item(i);
				if (node instanceof Element)
					return (Element) node;		
			}
		return null;
	}*/
	
	/**
	 * Protected function to return text context of the child element 
	 * @param parent - Parent element
	 * @param tagName - Tag name
	 * @return String - text context of element or null, if there is no such element or element does not have text context
	 */
	/*protected String getChildElementTextByTagName(Element parent, String tagName) {
		Element element = getChildElementByTagName(parent, tagName);
		if (null != element)
			return element.getTextContent();
		return null;
	}*/
		
	/**
	 * Protected function to check for error block in OAI:PMH response
	 * @param root - OAI:PMH Root element
	 * @return true, if there is an error block, false if not.
	 */
	/*protected boolean CheckForError(String xml) {
			System.out.println("Error in DOM structire, unable to extract root element");
			
			return false;
		}
		
		NodeList nl = root.getElementsByTagName("error");
		if (null != nl && nl.getLength() > 0)
		{
			Node node = nl.item(0);
			if (node instanceof Element) 
				System.out.println(String.format("Error code: %s, message: %s", 
						((Element) node).getAttribute("code"), 
						((Element) node).getTextContent()));
			
			else 
				System.out.println("Error in DOM structire, unable to extract error information");
				
			return false;
		}
		
		return true;
	}*/
	
	
	
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
		String url =  repoUrl + URL_LIST_SETS;
		
		try {
			Document doc = dbf.newDocumentBuilder().parse(url);
			
			Map<String, String> mapSets = new HashMap<String, String>();
			NodeList sets = (NodeList) XPATH_LIST_SETS.evaluate(doc, XPathConstants.NODESET);
			for (int i = 0; i < sets.getLength(); i++) {
				Node set = sets.item(i);
				String setName = (String) XPATH_SET_NAME.evaluate(set, XPathConstants.STRING);
				String setGroup = (String) XPATH_SET_SPEC.evaluate(set, XPathConstants.STRING);
				
				if (mapSets.put(setGroup, setName) != null)
					throw new Exception("The group already exists in the set");
			}
						
			return mapSets;						
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
		return null;
	}
	
	/**
	 * Protected function to find record in the index
	 * @param set - Set name
	 * @param key - Record key
	 * @return Record or null if there is no such record
	 */
	/*protected Record findRecord( final String set, final String key ) {
		return records.get(set + ":" + key);
	}*/
	
	/**
	 * Protected function to add record to the index
	 * @param record - Record to add
	 */
	/*protected void addRecord( Record record) {
		records.put(record.getSet() + ":" + record.getKey(), record);
	}*/
	
	/**
	 * Function to download records from the server. 
	 * If server will return resumption token, the function will return it, 
	 * otherways it will return null. If resumption token has been returned, 
	 * the function mast to be called again with this token, to download 
	 * next set of records.
	 * @param set name of the set
	 * @param metadataPrefix one of supported metadata prefix
	 * @param resumptionToken resuption token or null, if need to download first records from the set.
	 * @return String - Resumption Token or null, if there is no more records in the set
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws TransformerFactoryConfigurationError
	 * @throws TransformerException
	 */
		
	/**
	 * Function to download records from the server. 
	 * If server will return resumption token, the function will return it, 
	 * other ways it will return null. If resumption token has been returned, 
	 * the function mast to be called again with this token, to download 
	 * next set of records.
	 * This function will not parse the records and store all records as one file
	 * exactly same format as they has come from the server.
	 * This function also will newer use record index
	 * @param set name of the set
	 * @param metadataPrefix one of supported metadata prefix
	 * @param resumptionToken resuption token or null, if need to download first records from the set.
	 * @return String - Resumption Token or null, if there is no more records in the set
	 * @throws TransformerFactoryConfigurationError
	 * @throws Exception 
	 * @throws XPathExpressionException 
	 */
	public String downloadRecords( final String set, final String resumptionToken) throws Exception {
		// Generate the URL of request
		String url = null; ;
		if (null != resumptionToken) {
			try {
				url = repoUrl + String.format(URL_LIST_RECORDS_RESUMPTION_TOKEN, URLEncoder.encode(resumptionToken, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		if (null == url) {
			if (null == set)
				url = repoUrl + String.format(URL_LIST_DEFAULT_RECORDS, metadataPrefix);
			else
				url = repoUrl + String.format(URL_LIST_RECORDS,  URLEncoder.encode(set, "UTF-8"), metadataPrefix);
		}
		
		System.out.println("Downloading records: " + url);
		
		// Get XML document 
		URLConnection conn = new URL(url).openConnection();
		String xml = null;
	    try (InputStream is = conn.getInputStream()) {
	    	if (null != is) 
	    		xml = IOUtils.toString(is, StandardCharsets.UTF_8.name()); 
	    }
	    
	    // Check if xml has been returned and check what it had a valid root element
		if (null == xml) {
			System.err.println("The XML document is empty");
			return null;
		}
		
		String filePath = repoPrefix + "/" + metadataPrefix + "/" + harvestDate + "/" + (null == set ? "default" : set) + "/" + setOffset + ".xml";
		
		byte[] bytes = xml.getBytes(StandardCharsets.UTF_8);
		
		ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentEncoding(StandardCharsets.UTF_8.name());
        metadata.setContentType("text/xml");
        metadata.setContentLength(bytes.length);

        InputStream inputStream = new ByteArrayInputStream(bytes);

        PutObjectRequest request = new PutObjectRequest(bucketName, filePath, inputStream, metadata);

        s3client.putObject(request);
        String tokenString = null;

        Document doc;
        
		try {
			// Parse the xml 
			// if xml document is mailformed, this will throw an exception
			doc = dbf.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			
			// failback;
			return extractResumptionToken(xml);
					
		}
		// Extract root node
		Node root = (Node) XPATH_OAI_PMH.evaluate(doc, XPathConstants.NODE);
		if (null == root)
			throw new Exception("The document is not an OAI:PMH file");
	
		// Check for error node
		Node error = (Node) XPATH_ERROR.evaluate(root, XPathConstants.NODE); 
		if (null != error && error instanceof Element) {
			String code = ((Element) error).getAttribute("code");
			String message = ((Element) error).getTextContent();
		
			throw new Exception ("[" + code + "] " + message);
		}
				
		Node token = (Node) XPATH_RESUMPTION_TOKEN.evaluate(root, XPathConstants.NODE);
		
		if (null != token && token instanceof Element) {
			tokenString = ((Element) token).getTextContent();
			if (null != tokenString && !tokenString.isEmpty()) {
				String cursor = ((Element) token).getAttribute("cursor");
				String size = ((Element) token).getAttribute("completeListSize");
				try {
					setSize = Integer.parseInt(size);
				} catch(Exception e) {
					setSize = 0;
				}
				try {
					setOffset = Integer.parseInt(cursor);
				} catch(Exception e) {
					++setOffset;
				}
				System.out.println("ResumptionToken Detected. Cursor: " + setOffset + ", size: " + setSize);
				return tokenString;
			}
		}
		
		return null;
	}
	
	/*	private static final Pattern  = Pattern.compile(REG_RESUMPTION_TOKEN_START);
	private static final Pattern PATTERN_RESUMPTUON_TOKEN_END = Pattern.compile(REG_RESUMPTION_TOKEN_END);
	private static final Pattern PATTERN_RESUMPTUON_TOKEN_SIZE = Pattern.compile(REG_RESUMPTION_TOKEN_SIZE);
	private static final Pattern PATTERN_RESUMPTUON_TOKEN_CURSOR = Pattern.compile(REG_RESUMPTION_TOKEN_CURSOR);
	private static final Pattern PATTERN_RESUMPTUON_TOKEN_VALUE = Pattern.compile(REG_RESUMPTION_TOKEN_VALUE);
*/
	
	protected String extractResumptionToken(String xml) throws Exception {
		Matcher t = PATTERN_RESUMPTUON_TOKEN.matcher(xml);
		if (t.find()) {
			String token = t.group();
			
			Matcher s = PATTERN_RESUMPTUON_TOKEN_START.matcher(token);
			Matcher e = PATTERN_RESUMPTUON_TOKEN_END.matcher(token);
			if (s.find() && e.find()) {
				int start = s.end();
				int end = e.start();
				if (end > start) {
					String _token = token.substring(start, end);
					if (!StringUtils.isNullOrEmpty(_token)) {
						setSize = 0;
						++setOffset;
						
						return _token;
					}
				}
			} else 
				throw new Exception ("Error in Regular Expression");
		}
		
		return null;
	}
	
	/**
	 * Protected function to generate index file name
	 * @param indexName Set name
	 * @return path to index file
	 */
	/*protected String getIndexPath(final String indexName) {
		return folderXml + "/" + indexName + ".idx";
	}*/
	
	/**
	 * Protected function to generate set path
	 * @param setName Set name
	 * @return path to set folder
	 */
	/*protected String getSetPath(final String setName) {
		return folderXml + "/" + setName;
	}*/
	
	/**
	 * Protected function to generate set cache path
	 * @param setName Set name
	 * @return path to set cache folder
	 */
	/*protected String getCacheSetPath(final String setName) {
		return folderXml + "/_cache/" + setName;
	}*/
	
	/**
	 * Protected function to generate record file name
	 * @param keyName record identificator
	 * @return path to file
	 */
	/*protected String getFileNme(final String keyName)  {
		return keyName + ".xml";
	}*/
	
	/**
	 * Protected function to generate record file cache name
	 * @param keyName record identificator
	 * @param timestamp record timestamp
	 * @return path to cache file
	 */
	/*protected String getCacheFileName(final String keyName, final String timestamp)  {
		return keyName + "_" + timestamp + ".xml";
	}*/

	/**
	 * Protected function to open or create new index
	 * @param indexName index name
	 */
	/*protected void initIndex(final String indexName) {
		if (this.indexName == null || !this.indexName.equals(indexName)) {		
			records = new HashMap<String, Record>();
			
			try {
				FileInputStream f = new FileInputStream(getIndexPath(indexName));
				ObjectInputStream in = new ObjectInputStream(f);
				Record r = null;
				try {
					do {
						r = (Record) in.readObject();
						if (r != null)
							addRecord(r);
					} while (r != null);
				} finally {
					in.close();
					f.close();
				}		
				
			} catch (FileNotFoundException e) {
				System.out.println("Index file do not exists, new index has been created");
			} catch (IOException e) {
			} catch (ClassNotFoundException e) {
				System.out.println("Invalid index format");
				e.printStackTrace();
			}	
			
			this.indexName = indexName;
		}
	}*/
	
	/**
	 * Protected function to save and close index 
	 */
	/*protected void saveIndex() {
		if (null != this.indexName) {
			try {
				FileOutputStream f = new FileOutputStream(getIndexPath(this.indexName));
				ObjectOutputStream out = new ObjectOutputStream(f);
				try {
					for (Record record : records.values()) {
					    out.writeObject(record);
					}
				} finally {
					out.close();
					f.close();
				}				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				this.indexName = null;
			}
		}
	}*/
	
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
	public void harvest() throws Exception {	
		if (StringUtils.isNullOrEmpty(metadataPrefix))
			throw new IllegalArgumentException("The OAI:PMH Metadata Prefix can not be empty");
		
	//	File fileStatus = new File(repoPrefix + "_" + metadataPrefix + "_status.xml");
	//	Status status = loadStatus(fileStatus);

		System.out.println("Downloading set list");
		
		// download all sets in the repository
		Map<String, String> mapSets = listSets();
		
		if (null == mapSets || mapSets.isEmpty()) {
			System.out.println("Processing deafult set");

			harvestSet(null);
		} else {
			
			for (Map.Entry<String, String> entry : mapSets.entrySet()) {
			    
				String set = entry.getKey();
			    
			    // if black list exists and item is blacklisted, continue
				if (null != whiteList && !whiteList.isEmpty()) {
				    if (!whiteList.contains(set))
				    	continue;					
				} else if (null != blackList && blackList.contains(set))
			    	continue;			    
			    
			    System.out.println("Processing set: " +  URLDecoder.decode(entry.getValue(), StandardCharsets.UTF_8.name()));
			    
			    harvestSet(null);
			}
			
		/*if (null == mapSets || mapSets.isEmpty()) {
			
			setSize = 0;
		    setOffset = 0;
		    
		    if (null != status.getCurrentSet() && status.getCurrentSet().equals("default")) {
		    	resumptionToken = status.getResumptionToken();
		    	
		    	setSize = status.getSetSize();
		    	setOffset = status.getSetOffset();
		    } else {
		    	status.setCurrentSet("default");
		    	status.setResumptionToken(null);
		    	status.setSetSize(0);
		    	status.setSetOffset(0);
		    	
		    	resumptionToken = null;
		    }
		    
		    System.out.println("Processing deafult set");

		    do {
		    	try {
		    		resumptionToken = downloadRecords(null, resumptionToken);		
		    		
		    		if (null != resumptionToken && !resumptionToken.isEmpty()) {
		    			status.setResumptionToken(resumptionToken);
		    			status.setSetSize(setSize);
		    			status.setSetOffset(setOffset);
		    			saveStatus(status, fileStatus);
		    		}		    		
		    	}
		    	catch (Exception e) {
		    		System.out.println("Error downloading data");
		    		
		    		e.printStackTrace();
		    		
		    		resumptionToken = null;
		    	}
		    	
		    } while (null != resumptionToken && !resumptionToken.isEmpty());		 
		    
		    if (fileStatus.exists())
				fileStatus.delete();
			
		} else {
			// try to load whole database into memory
			for (Map.Entry<String, String> entry : mapSets.entrySet()) {
			    String set = entry.getKey();
			    if (status.getProcessedSets().contains(set))
			    	continue;
			    
			    setSize = 0;
			    setOffset = 0;
			    
			    if (null != status.getCurrentSet() && status.getCurrentSet().equals(set)) {
			    	resumptionToken = status.getResumptionToken();
			    	
			    	setSize = status.getSetSize();
			    	setOffset = status.getSetOffset();
			    } else {
			    	status.setCurrentSet(set);
			    	status.setResumptionToken(null);
			    	status.setSetSize(0);
			    	status.setSetOffset(0);
			    	
			    	resumptionToken = null;
			    }
			    
			    String setName = entry.getValue();
			    
			    System.out.println("Processing set: " +  URLDecoder.decode(setName, StandardCharsets.UTF_8.name()));
			    		    
			    do {
			    	try {
			    		resumptionToken = downloadRecords(set, resumptionToken);		
			    		
			    		if (null != resumptionToken && !resumptionToken.isEmpty()) {
			    			status.setResumptionToken(resumptionToken);
			    			status.setSetSize(setSize);
			    			status.setSetOffset(setOffset);
			    			saveStatus(status, fileStatus);
			    		}
			    		
			    	}
			    	catch (Exception e) {
		    			System.out.println("Error downloading data");
			    		
		    			e.printStackTrace();
		    			
		    			resumptionToken  = null;
			    	}
			    	
			    } while (null != resumptionToken && !resumptionToken.isEmpty());		 
			    
			    status.addProcessedSet(set);
			    status.setCurrentSet(null);
			    status.setResumptionToken(null);
			    status.setSetSize(0);
			    status.setSetOffset(0);
			    saveStatus(status, fileStatus);
			}
			
			if (fileStatus.exists())
				fileStatus.delete();
				*/
		}	
		
		String filePath = repoPrefix + "/" + metadataPrefix + "/latest.txt";
		
		byte[] bytes = harvestDate.getBytes(StandardCharsets.UTF_8);
		
		ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentEncoding(StandardCharsets.UTF_8.name());
        metadata.setContentType("text/xml");
        metadata.setContentLength(bytes.length);

        InputStream inputStream = new ByteArrayInputStream(bytes);

        PutObjectRequest request = new PutObjectRequest(bucketName, filePath, inputStream, metadata);

        s3client.putObject(request);
	}
	
	private void harvestSet(String set) throws Exception {
		String resumptionToken = null;
		
		setSize = 0;
		setOffset = 0;
		
		do {
	    	resumptionToken = downloadRecords(set, resumptionToken);					    	
	    } while (null != resumptionToken && !resumptionToken.isEmpty());
	}
	
	/**
	 * Function to get Repo URL
	 * @return String - Repo URL
	 */
	public String getRepoUrl() {
		return repoUrl;
	}

	/**
	 * Function to get base folder path
	 * @return String - base folder
	 */
	/*public String getFolderBase() {
		return folderBase;
	}*/

	/**
	 * Function to set Repo URL
	 * @param repoUrl an Repo URL
	 */
	public void setRepoUrl(String repoUrl) {
		this.repoUrl = repoUrl;
	}

	/**
	 * Function to set base folder base
	 * @param folderBase base folder path
	 */
	/*public void setFolderBase(String folderBase) {
		this.folderBase = folderBase;
	}*/
	
	/**
	 * Protected function to load harvesting status
	 * @param file a harvest status file
	 * @return Status - Harvesting status
	 */
	/*protected Status loadStatus(File file) {
        try {
            if (file.exists() && !file.isDirectory())
            	return (Status) jaxbUnmarshaller.unmarshal(file);
        } catch (Exception e) {
                e.printStackTrace();
        }
        
        return new Status();
	}*/
	 
	/**
	 * Protected function to save harvesting status 
	 * @param status Harvesting status
	 * @param file Status file
	 */
	
	/*protected void saveStatus(Status status, File file) {
        try {
            jaxbMarshaller.marshal(status, file);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}  */ 
}