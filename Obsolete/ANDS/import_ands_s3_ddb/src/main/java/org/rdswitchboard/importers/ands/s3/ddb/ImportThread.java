package org.rdswitchboard.importers.ands.s3.ddb;

import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.Semaphore;

import javax.xml.bind.JAXBException;

import org.rdswitchboard.crosswalks.rifcs.graph.Crosswalk;
import org.rdswitchboard.importers.graph.ddb.Importer;
import org.rdswitchboard.libraries.record.Record;

import com.amazonaws.auth.AWSCredentials;

public class ImportThread  extends Thread {

	private final Object lock = new Object();
	private boolean canExit = false;
	private final Semaphore semaphore;
	private final Crosswalk crosswalk;
	private final Importer importer;
	private final String source;
	private InputStream stream = null; 
	
	public ImportThread(String source, Semaphore semaphore, AWSCredentials awsCredentials) throws JAXBException {
		this.source = source;
		this.semaphore = semaphore;
		this.crosswalk = new Crosswalk();
		this.importer = new Importer(awsCredentials);
	}
	
	public boolean isFree() {
		synchronized(lock) {
			return null == this.stream;
		}
	}
	
	public void process(InputStream stream) throws ImportThreadException {
		synchronized(lock) {
			if (null != this.stream)
				throw new ImportThreadException("The Imput tread is busy");
		
			this.stream = stream;
			this.lock.notify();
		}
	}
	
	public synchronized void finishCurrentAndExit() {
		synchronized(lock) {
			
			this.canExit = true;
			this.lock.notify();
		}
	}
	
	@Override
	public void run() {
		try {
			 for (;;) {
				 synchronized(lock) {
					 if (canExit)
						 break;
					 
					 if (null == stream) 
						 lock.wait();
				 }
				 
				 if (null != stream) {
					try {
						Collection<Record> records = crosswalk.process(stream).values();
						importer.importRecords(source, records);
					} catch (Exception e) {
						e.printStackTrace();
					}
					 
					stream = null;
					semaphore.release();
				 }
			 }
		} catch (InterruptedException e) {
	    }
    }
} 