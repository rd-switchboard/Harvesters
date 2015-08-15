package org.rdswitchboard.libraries.records;

import java.util.List;

public interface Records {
	void addRecord(Record record);
	void addRelationship(Relationship relationship);
	List<Record> getRecords();
	List<Relationship> getRelationships();
}
