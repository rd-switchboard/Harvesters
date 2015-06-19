package org.rdswitchboard.harvesters.pmh;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


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
	
	protected static final String URL_IDENTIFY = "?verb=Identify";
	protected static final String URL_LIST_METADATA_FORMATS = "?verb=ListMetadataFormats";
	protected static final String URL_LIST_SETS = "?verb=ListSets";
	protected static final String URL_LIST_RECORDS = "?verb=ListRecords&set=%s&metadataPrefix=%s";
	protected static final String URL_LIST_RECORDS_RESUMPTION_TOKEN = "?verb=ListRecords&resumptionToken=%s";

	protected static final String ELEMENT_ROOT = "OAI-PMH";
	
	protected static final String XPATH_RESUMPTION_TOKEN = "/OAI-PMH/ListRecords/resumptionToken";

	/**
	 * variable to store repo URL. Can not be null.
	 */
	protected String repoUrl;
	
	/**
	 * Variable to store base folder for harvested data. Can not be null.
	 */
	protected String folderBase;
	
	/**
	 * Variable to store folder for current meta data. Will be automatically 
	 * initialized after harvesting process will be started.
	 */
	protected String folderXml;

	/**
	 * Variable to store current index name as {@code<folderXml>/<setName>.idx} .
	 */
	protected String indexName;
	
	/**
	 * File encoding (UTF-8 is default)
	 */
	protected String encoding;

	/**
	 * Variable to store current set index
	 */
	protected Map<String, Record> records;
	
	/**
	 * Document builder factory to load and parse XML documents
	 */
	protected DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	
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
	
	private int setSize;
	private int setOffset;
	
	private JAXBContext jaxbContext;
	private Marshaller jaxbMarshaller;
	private Unmarshaller jaxbUnmarshaller;
	
	private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
	private TransformerFactory transformerFactory = TransformerFactory.newInstance();
	/**
	 * Harvester constructor
	 * 
	 * @param repoUrl : The Repository URL
	 * @param folderBase : The address of the folder, there received data must be saved.
	 * @throws JAXBException
	 */
	public Harvester( final String repoUrl, final String folderBase ) throws JAXBException {
		this.repoUrl = repoUrl;
		this.folderBase = folderBase;
	
		jaxbContext = JAXBContext.newInstance(Status.class);
		jaxbMarshaller = jaxbContext.createMarshaller();
		jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		jaxbUnmarshaller = jaxbContext.createUnmarshaller();
	
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
	
	/**
	 * Return encoding (if null, the default UTF-8 encoding will be used)
	 * @return String - encoding
	 */
	public String getEncoding() {
		return encoding;
	}

	/**
	 * Set encoding (UTF-8 is by default)
	 * @param encoding String
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
	/**
	 * Protected function to return XML Document
	 * @param uri resource URI
	 * @return Document - the loaded document
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	protected Document GetXml( final String uri ) throws ParserConfigurationException, SAXException, IOException {
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
	}

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
	protected void printDocument(Document doc, OutputStream out) 
			throws IOException, TransformerException {
	    Transformer transformer = transformerFactory.newTransformer();
	    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
	    transformer.setOutputProperty(OutputKeys.METHOD, "xml");
	    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
	    transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
	    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
	
	    transformer.transform(new DOMSource(doc), 
	         new StreamResult(new OutputStreamWriter(out, "UTF-8")));
	}
	
	/**
	 * Protected function to find child element by tag name 
	 * @param parent Parent element
	 * @param tagName Tag name
	 * @return Element - Child element or null if such element could not be found
	 */
	protected Element getChildElementByTagName(Element parent, String tagName) {
	    for(Node child = parent.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element && tagName.equals(((Element) child).getTagName())) 
	        	return (Element) child;
	    return null;
	}
	
	/**
	 * Protected function to find child elements by tag name 
	 * @param parent Parent element
	 * @param tagName Tag name
	 * @return {@code List<Element>} - List of child elements or null if such element could not be found
	 */
	protected List<Element> getChildElementsByTagName(Element parent, String tagName) {
		List<Element> elements = null;
		for(Node child = parent.getFirstChild(); child != null; child = child.getNextSibling())
	        if(child instanceof Element && tagName.equals(((Element) child).getTagName())) {
	        	if (null == elements)
	        		elements = new ArrayList<Element>();
	        	elements.add((Element) child);
	        }
	    return elements;
	}
	
	/**
	 * Protected function to return Document element by tag name
	 * @param doc - XML Document
	 * @param tagName - Tag name
	 * @return Element - Element or null if such element could not be found
	 */
	protected Element getDocumentElementByTagName(Document doc, String tagName) {
		NodeList list = doc.getElementsByTagName(tagName);
		if (null != list)
			for (int i = 0; i < list.getLength(); ++i) {
				Node node = list.item(i);
				if (node instanceof Element)
					return (Element) node;		
			}
		return null;
	}
	
	/**
	 * Protected function to return text context of the child element 
	 * @param parent - Parent element
	 * @param tagName - Tag name
	 * @return String - text context of element or null, if there is no such element or element does not have text context
	 */
	protected String getChildElementTextByTagName(Element parent, String tagName) {
		Element element = getChildElementByTagName(parent, tagName);
		if (null != element)
			return element.getTextContent();
		return null;
	}
		
	/**
	 * Protected function to check for error block in OAI:PMH response
	 * @param root - OAI:PMH Root element
	 * @return true, if there is an error block, false if not.
	 */
	protected boolean CheckForError(Element root) {
		if (null == root) {
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
	}
	
	/**
	 * Function to indentify on OAI:PMH Server. Could be used to test connection with the server.
	 * Also will initialize all server information variables.
	 * @return true if connection could be established and server didn't return any error.
	 */
	public boolean identify() {
		String url =  repoUrl + URL_IDENTIFY;
		
		try {
			Document doc = GetXml(url);
			// printDocument(doc, System.out);
			Element root = getDocumentElementByTagName(doc, ELEMENT_ROOT);
			Element identify = getChildElementByTagName(root, "Identify");
				
			//responseDate = root.getElementsByTagName("responseDate").item(0).getTextContent();			

			repositoryName = getChildElementTextByTagName(identify, "repositoryName");
			protocolVersion = getChildElementTextByTagName(identify, "protocolVersion");
			earliestTimestamp = getChildElementTextByTagName(identify, "earliestTimestamp");
			deletedRecord = getChildElementTextByTagName(identify, "deletedRecord");
			granularity = getChildElementTextByTagName(identify, "granularity");
			adminEmail = getChildElementTextByTagName(identify, "adminEmail");
			
			return true;
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
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
			return MetadataFormat.getMetadataFormats(GetXml(url));
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
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
			Document doc = GetXml(url);
			Element root = getDocumentElementByTagName(doc, ELEMENT_ROOT);
			List<Element> sets = getChildElementsByTagName(
					getChildElementByTagName(root, "ListSets"), "set");
			if (null == sets)
				throw new Exception("The sets collection is empty");
			Map<String, String> mapSets = null; 
			
			for (Element set : sets) {
				String setName = getChildElementTextByTagName(set, "setName");
				String setGroup = getChildElementTextByTagName(set, "setSpec");
				
				if (null == mapSets)
					mapSets = new HashMap<String, String>();
				
				if (mapSets.put(setGroup, setName) != null)
					throw new Exception("The group already exists in the set");
			}
			
			return mapSets;						
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
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
	protected Record findRecord( final String set, final String key ) {
		return records.get(set + ":" + key);
	}
	
	/**
	 * Protected function to add record to the index
	 * @param record - Record to add
	 */
	protected void addRecord( Record record) {
		records.put(record.getSet() + ":" + record.getKey(), record);
	}
	
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
	public String downloadRecords( final String set, final MetadataPrefix metadataPrefix, 
			final String resumptionToken ) throws ParserConfigurationException, SAXException, 
			IOException, TransformerFactoryConfigurationError, TransformerException {
				
		// create set folder
		String setName = URLEncoder.encode(set, "UTF-8");
		String setPath = null; //getSetPath(setName);
		String setCachePath = null;
	//	new File(setPath).mkdirs();
		
		String url = repoUrl;	
		if (null != resumptionToken) {
			try {
				url += String.format(URL_LIST_RECORDS_RESUMPTION_TOKEN, URLEncoder.encode(resumptionToken, "UTF-8"));
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
		}
		else
			url += String.format(URL_LIST_RECORDS, set,  metadataPrefix.name());
		System.out.println(url);
		
		Document doc  = GetXml(url);
			
		Element root = getDocumentElementByTagName(doc, ELEMENT_ROOT);
		
		if (!CheckForError(root))
			return null;
			
		/*	
			String fileName = "xml/" + set;
			if (null != offset)
				fileName += "_" + offset;
			fileName +=  ".xml"; 
			
			transformer.transform(new DOMSource(doc), new StreamResult(new File(fileName)));*/
		Transformer transformer = transformerFactory.newTransformer();
			
		List<Element> records = getChildElementsByTagName(getChildElementByTagName(root, "ListRecords"), "record");
		if (null != records) {
			
			new File(setPath = getSetPath(setName)).mkdirs();
			initIndex(setName);	
			
			for (Element record : records) {				
				Element header = getChildElementByTagName(record, "header");
				String identifier = getChildElementTextByTagName(header, "identifier");
				String datestamp = getChildElementTextByTagName(header, "datestamp");
				
				Record rec = findRecord(set, identifier);
				if (null == rec || !rec.getDate().equals(datestamp)) {
					// the file missing in the index, or file date is different
					
					String keyName = URLEncoder.encode(identifier, "UTF-8");
					String fineName = getFileNme(keyName);
					
					String filePath = setPath + "/" + fineName;
					
					if (rec != null) {
						// file is different, copy the old file into the cashe
						if (null == setCachePath) 
							new File(setCachePath = getCacheSetPath(setName)).mkdirs();
												
						new File(filePath).renameTo(new File(setCachePath + "/" + getCacheFileName(keyName, datestamp)));
					} else {
						rec = new Record();
						rec.setSet(set);
						rec.setKey(identifier);
					//	rec.file = setName + "/" + fineName;
						
						addRecord(rec);
					}
					
					rec.setDate(datestamp);
					
					transformer.transform(new DOMSource(record), new StreamResult(filePath));						
				}
			}	
		
			saveIndex();
		}
			
		NodeList nl = doc.getElementsByTagName("resumptionToken");
		if (nl != null && nl.getLength() > 0)
		{
			Element token = (Element) nl.item(0);
			String tokenString = token.getTextContent();
			if (null != tokenString && !tokenString.isEmpty()) {
				String cursor = token.getAttribute("cursor");
				String size = token.getAttribute("completeListSize");
				
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
				
				return token.getTextContent();
			}
		}
		
		return null;
	}	
	
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
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws TransformerFactoryConfigurationError
	 * @throws TransformerException
	 * @throws XPathExpressionException 
	 */
	public String downloadRecordsSimple( final String set, final MetadataPrefix metadataPrefix,
			final String resumptionToken) throws ParserConfigurationException, SAXException,
			IOException, TransformerFactoryConfigurationError, TransformerException {
	
		// create set folder
		String setName = URLEncoder.encode(set, "UTF-8");
		String url = repoUrl;
		if (null != resumptionToken) {
			try {
				url += String.format(URL_LIST_RECORDS_RESUMPTION_TOKEN, URLEncoder.encode(resumptionToken, "UTF-8"));
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
		}
		else
			url += String.format(URL_LIST_RECORDS, set, metadataPrefix.name());
		
		System.out.println(url);
		
		// Get XML document and parse it
		Document doc = GetXml(url);
		
		// Extract root element
		Element root = getDocumentElementByTagName(doc, ELEMENT_ROOT);
		
		// check for errors
		if (!CheckForError(root))
			return null;
		
		// Create new transformer
		Transformer transformer = transformerFactory.newTransformer();
		
		// Create set path
		String setPath = getSetPath(setName);
		new File(setPath).mkdirs();
		
		// Create file name and temporary file name
		String filePath = setPath + "/" + setOffset + ".xml";
		String fileTmp = setPath + "/" + setOffset + ".tmp";
		
		// Output file as temporary
		transformer.transform(new DOMSource(doc), new StreamResult(fileTmp));
		
		// Create file ojects
		File fPath = new File(filePath);
		File fTmp = new File(fileTmp);
		
		// Check that file already exists
		if (fPath.exists()) {
			// Compare file sizes
			if (fPath.length() != fTmp.length())
			{
				// if file size are different, move old file to cache, if it is same, just delete the old file
				String setCachePath = getCacheSetPath(setName);
				new File(setCachePath).mkdirs();
		
				fPath.renameTo(new File(setCachePath + "/" + setOffset + "_" + dateFormat.format(new Date()) + ".xml"));
			}
			else
				fPath.delete();
			
			// restore path name (might be not needed)
			// fPath = new File(filePath);
		}
		fTmp.renameTo(fPath);
		NodeList nl = doc.getElementsByTagName("resumptionToken");
		if (nl != null && nl.getLength() > 0)
		{
			Element token = (Element) nl.item(0);
			String tokenString = token.getTextContent();
			if (null != tokenString && !tokenString.isEmpty()) {
				String cursor = token.getAttribute("cursor");
				String size = token.getAttribute("completeListSize");
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
				return token.getTextContent();
			}
		}
		return null;
	}
	
	/**
	 * Protected function to generate index file name
	 * @param indexName Set name
	 * @return path to index file
	 */
	protected String getIndexPath(final String indexName) {
		return folderXml + "/" + indexName + ".idx";
	}
	
	/**
	 * Protected function to generate set path
	 * @param setName Set name
	 * @return path to set folder
	 */
	protected String getSetPath(final String setName) {
		return folderXml + "/" + setName;
	}
	
	/**
	 * Protected function to generate set cache path
	 * @param setName Set name
	 * @return path to set cache folder
	 */
	protected String getCacheSetPath(final String setName) {
		return folderXml + "/_cache/" + setName;
	}
	
	/**
	 * Protected function to generate record file name
	 * @param keyName record identificator
	 * @return path to file
	 */
	protected String getFileNme(final String keyName)  {
		return keyName + ".xml";
	}
	
	/**
	 * Protected function to generate record file cache name
	 * @param keyName record identificator
	 * @param timestamp record timestamp
	 * @return path to cache file
	 */
	protected String getCacheFileName(final String keyName, final String timestamp)  {
		return keyName + "_" + timestamp + ".xml";
	}

	/**
	 * Protected function to open or create new index
	 * @param indexName index name
	 */
	protected void initIndex(final String indexName) {
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
	}
	
	/**
	 * Protected function to save and close index 
	 */
	protected void saveIndex() {
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
	public void harvest(MetadataPrefix prefix) throws Exception {
		
		folderXml = folderBase + "/" + prefix.name();
		new File(folderXml).mkdirs();
		
		File fileStatus = new File(folderXml + "/status.xml");
		Status status = loadStatus(fileStatus);
	
		System.out.println("Downloading set list...");
		
		Map<String, String> mapSets = listSets();
		if (null == mapSets )
			throw new Exception("The sets collection is empty");
		
			// try to load whole database into memory
		for (Map.Entry<String, String> entry : mapSets.entrySet()) {
		    String set = entry.getKey();
		    if (status.getProcessedSets().contains(set))
		    	continue;
		    
		    setSize = 0;
		    setOffset = 0;
		    
		    String resumptionToken = null;
		    if (null != status.getCurrentSet() && status.getCurrentSet().equals(set)) {

		    	setSize = status.getSetSize();
		    	setOffset = status.getSetOffset();
		    	
		    	resumptionToken = status.getResumptionToken();
		    } else {
		    	status.setCurrentSet(set);
		    	status.setResumptionToken(null);
		    	status.setSetSize(0);
		    	status.setSetOffset(0);
		    
		   // 	saveStatus(status, fileStatus);
		    }
		    
		    String setName = entry.getValue();
		    
		    System.out.println("Processing set: " +  URLDecoder.decode(setName, "UTF-8"));
		    
		    int nError = 0;
		    do {
		    	try {
		    		resumptionToken = downloadRecords(set, prefix, resumptionToken);		
		    		
		    		if (null != resumptionToken && !resumptionToken.isEmpty()) {
		    			status.setResumptionToken(resumptionToken);
		    			status.setSetSize(setSize);
		    			status.setSetOffset(setOffset);
		    			saveStatus(status, fileStatus);
		    		}
		    		
		    		nError = 0;		    		
		    	}
		    	catch (Exception e) {
		    		if (++nError >= 10) {
		    			System.out.println("Too much errors has been detected, abort download");
		    			
		    			throw e;
		    		} else { 
		    			System.out.println("Error downloading data");
		    		
		    			e.printStackTrace();
		    		}
		    	}
		    	
		    } while (nError > 0 || null != resumptionToken && !resumptionToken.isEmpty());		 
		    
		    status.addProcessedSet(set);
		    status.setCurrentSet(null);
		    status.setResumptionToken(null);
		    status.setSetSize(0);
		    status.setSetOffset(0);
		    saveStatus(status, fileStatus);
		}
		
		if (fileStatus.exists())
			fileStatus.delete();
	}
	
	/**
	 * Alternative function to organize the harvest process. The difference with another function
	 * is in data storage. The harvest2 function will store files in the raw format as they come
	 * from the server.
	 * The harvesting method should never be mixed. The harvesting folder must be wiped out if 
	 * switching to this method, or function will fail.
	 * @param prefix A metadata prefix
	 * @throws Exception
	 */
	public void harvestSimple(MetadataPrefix prefix) throws Exception {
		
		folderXml = folderBase + "/" + prefix.name();
		new File(folderXml).mkdirs();
		
		File fileStatus = new File(folderXml + "/status.xml");
		Status status = loadStatus(fileStatus);
	
		System.out.println("Downloading set list...");
		
		Map<String, String> mapSets = listSets();
		if (null == mapSets )
			throw new Exception("The sets collection is empty");
		
			// try to load whole database into memory
		for (Map.Entry<String, String> entry : mapSets.entrySet()) {
		    String set = entry.getKey();
		    if (status.getProcessedSets().contains(set))
		    	continue;
		    
		    String resumptionToken = null;
		    
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
		    
		   // 	saveStatus(status, fileStatus);
		    }
		    
		    String setName = entry.getValue();
		    
		    System.out.println("Processing set: " +  URLDecoder.decode(setName, "UTF-8"));
		    		    
		    int nError = 0;
		    do {
		    	try {
		    		resumptionToken = downloadRecordsSimple(set, prefix, resumptionToken);		
		    		
		    		if (null != resumptionToken && !resumptionToken.isEmpty()) {
		    			status.setResumptionToken(resumptionToken);
		    			status.setSetSize(setSize);
		    			status.setSetOffset(setOffset);
		    			saveStatus(status, fileStatus);
		    		}
		    		
		    		nError = 0;		    		
		    	}
		    	catch (Exception e) {
		    		if (++nError >= 10) {
		    			System.out.println("Too much errors has been detected, abort download");
		    			
		    			throw e;
		    		} else { 
		    			System.out.println("Error downloading data");
		    		
		    			e.printStackTrace();
		    		}
		    	}
		    	
		    } while (nError > 0 || null != resumptionToken && !resumptionToken.isEmpty());		 
		    
		    status.addProcessedSet(set);
		    status.setCurrentSet(null);
		    status.setResumptionToken(null);
		    status.setSetSize(0);
		    status.setSetOffset(0);
		    saveStatus(status, fileStatus);
		}
		
		if (fileStatus.exists())
			fileStatus.delete();
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
	public String getFolderBase() {
		return folderBase;
	}

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
	public void setFolderBase(String folderBase) {
		this.folderBase = folderBase;
	}
	
	/**
	 * Protected function to load harvesting status
	 * @param file a harvest status file
	 * @return Status - Harvesting status
	 */
	protected Status loadStatus(File file) {
		try {
			if (file.exists() && !file.isDirectory())
				return (Status) jaxbUnmarshaller.unmarshal(file);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return new Status();
	}
	
	/**
	 * Protected function to save harvesting status 
	 * @param status Harvesting status
	 * @param file Status file
	 */
	
	protected void saveStatus(Status status, File file) {
		try {
			jaxbMarshaller.marshal(status, file);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
