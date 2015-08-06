package org.rdswitchboard.utils.rds.solr.sync;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Analyze {
	private final int total;
	private final int chunkSize;
	private final int numChunk;
	
	@JsonCreator
	public Analyze(
			@JsonProperty("total") int total, 
			@JsonProperty("chunkSize") int chunkSize, 
			@JsonProperty("numChunk") int numChunk) {
		this.total = total;
		this.chunkSize = chunkSize;
		this.numChunk = numChunk;
	}

	public int getTotal() {
		return total;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public int getNumChunk() {
		return numChunk;
	}

	@Override
	public String toString() {
		return "Analyze [total=" + total + ", chunkSize=" + chunkSize
				+ ", numChunk=" + numChunk + "]";
	}
}
