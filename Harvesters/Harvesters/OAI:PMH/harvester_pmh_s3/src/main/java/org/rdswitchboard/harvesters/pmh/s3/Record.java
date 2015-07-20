package org.rdswitchboard.harvesters.pmh.s3;

import java.io.Serializable;

/**
 * Class to store single record
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 *
 */
public class Record implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3957349468305765200L;
	
	private String set;
	private String key;
	private String date;
	
	/**
	 * @return true if record is valid
	 */
	public boolean isValid() {
		return !set.isEmpty() && !key.isEmpty(); 
	}
	
	/**
	 * @return String - set name
	 */
	public String getSet() {
		return set;
	}

	/**
	 * Set set name
	 * @param set String 
	 */
	public void setSet(String set) {
		this.set = set;
	}

	/**
	 * @return String - key
	 */
	public String getKey() {
		return key;
	}

	/**
	 * Set key name
	 * @param key String
	 */
	public void setKey(String key) {
		this.key = key;
	}

	/**
	 * @return string - date
	 */
	public String getDate() {
		return date;
	}

	/**
	 * Set date
	 * @param date String
	 */
	public void setDate(String date) {
		this.date = date;
	}

	@Override
	public String toString() {
		return "Record [set=" + set + ", key=" + key + ", date=" + date + "]";
	}
}
