package org.rdswitchboard.libraries.graph;

import java.io.InputStream;

import javax.xml.bind.JAXBException;

public interface GraphCrosswalk {
	Graph process(String source, InputStream xml) throws Exception;
}
