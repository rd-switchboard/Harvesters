package org.rdswitchboard.importers.ands.s3.ddb;

public class ImportThreadException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4347715838831919266L;

	public ImportThreadException(String reason) {
		super ("Import Thread Exception: " + reason);
	}
}