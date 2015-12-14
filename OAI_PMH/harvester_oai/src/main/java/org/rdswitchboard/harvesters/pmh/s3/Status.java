package org.rdswitchboard.harvesters.pmh.s3;

import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Class to store harvesting status
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */

/*
@XmlRootElement
public class Status {
	private Set<String> processedSets;
	private String currentSet;
	private String resumptionToken;
	private int setSize;
	private int setOffset;
	
	/**
	 * Return collecton of processed sets
	 * @return {@code Set<String>} - set if processed sets
	 * /
	public Set<String> getProcessedSets() {
		if (null == processedSets)
			processedSets = new HashSet<String>();
		return processedSets;
	}
	
	/**
	 * Init collection of processed sets
	 * @param processedSets {@code Set<String>}
	 * /
	@XmlElement
	public void setProcessedSets(Set<String> processedSets) {
		this.processedSets = processedSets;
	}
	
	/**
	 * Add set to collection
	 * @param processedSet String
	 * /
	public void addProcessedSet(String processedSet) {
		getProcessedSets().add(processedSet);
	}

	/**
	 * Returns current set
	 * @return String - current set
	 * /
	public String getCurrentSet() {
		return currentSet;
	}

	/**
	 * Store current set
	 * @param currentSet - String
	 * /
	@XmlElement
	public void setCurrentSet(String currentSet) {
		this.currentSet = currentSet;
	}

	/**
	 * Returns resumption token
	 * @return String - resumption token
	 * /
	public String getResumptionToken() {
		return resumptionToken;
	}

	/**
	 * Store resumption token
	 * @param resumptionToken - String
	 * /
	@XmlElement
	public void setResumptionToken(String resumptionToken) {
		this.resumptionToken = resumptionToken;
	}
	
	public int getSetSize() {
		return setSize;
	}

	@XmlElement
	public void setSetSize(int setSize) {
		this.setSize = setSize;
	}

	public int getSetOffset() {
		return setOffset;
	}

	@XmlElement
	public void setSetOffset(int setOffset) {
		this.setOffset = setOffset;
	}

	@Override
	public String toString() {
		return "Status [processedSets=" + processedSets + ", currentSet="
				+ currentSet + ", resumptionToken=" + resumptionToken + "]";
	}
}*/
