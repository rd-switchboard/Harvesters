package org.rdswitchboard.harvesters.rda;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Record Set class
 * @author Dmitrij Kudriavcev, dmitrij@kudriavcev.info
 */
public class RecordSet {
	protected static final String FIELD_NUM_FOUND = "numFound";
	protected static final String FIELD_START = "start";
	protected static final String FIELD_DOCS = "docs";
	protected static final String FIELD_ID = "id";
	
	private Set<String> recordIds = new HashSet<String>();
	private Integer from;
	private Integer found;
	private Integer processed;
	
	/**
	 * Class constructor from json data
	 * @param json {@code Map<String, Object>} : A map of JSON dara
	 * @return RecordSet
	 * @throws Exception
	 */
	public static RecordSet fromJson(Map<String, Object> json) throws Exception {
		// fist check that we have docs element in the response
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> docs = (List<Map<String, Object> >) json.get(FIELD_DOCS);
		if (null !=  docs) {
			// create RecorSet
			RecordSet recordSet = new RecordSet();
			recordSet.found = (Integer) json.get(FIELD_NUM_FOUND);
			recordSet.from = (Integer) json.get(FIELD_START);
			recordSet.processed = docs.size();
			
			for (Map<String, Object> doc : docs) {
				String id = (String) doc.get(FIELD_ID);
				if (null != id && !id.isEmpty())
					recordSet.recordIds.add(id);
			}
			
			return recordSet;
		} else
			throw new Exception("Invalid response format, unbale to find obects array");
	}
	
	/**
	 * @return Set of Records IDs
	 */
	public Set<String> getRecordIds() {
		return recordIds;
	}

	/**
	 * Return start index
	 * @return Integer
	 */
	public Integer getFrom() {
		return from;
	}

	/**
	 * Return number of found records
	 * @return Integer
	 */
	public Integer getFound() {
		return found;
	}

	/**
	 * Return number of processed records
	 * @return Integer
	 */
	public Integer getProcessed() {
		return processed;
	}

	@Override
	public String toString() {
		return "RecordSet [recordIds=" + recordIds + ", from=" + from
				+ ", found=" + found + ", processed=" + processed + "]";
	}	
}
