package org.rdswitchboard.harvesters.pmh.s3;

import java.io.PrintStream;

/**
 * @author dima
 *
 */
public class SetStatus {
	
	private String name;
	private String title;
	private String error;
	private String token = null;
	private int cursor;
	private int size;
	private int files;
	private int records;
	private long milliseconds;
	
	public SetStatus(String name, String title) {
		this.name = name;
		this.title = title;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean hasName() {
		return null != name && !name.isEmpty();
	}
		
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getToken() {
		return token;
	}
	
	public void setToken(String token) {
		this.token = token;
	}
	
	public boolean hasToken() {
		return null != token && !token.isEmpty();
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
		this.files = -3;
	}
	
	public boolean hasError() {
		return this.files == -3;
	}

	public int getFiles() {
		return files;
	}
	
	public void setFiles(int files) {
		this.files = files;
	}
	
	public void incFiles() {
		++files;
	}
	
	public int getRecords() {
		return records;
	}
	
	public void setRecords(int records) {
		this.records = records;
	}
	
	public long getMilliseconds() {
		return milliseconds;
	}
	
	public String getEllapsedTime() {
		long t = milliseconds; 
		long ms = t % 1000;
		t /= 1000;
		long s = t % 60;
		t /= 60;
		long m = t % 60;
		t /= 60;
		long h = t % 24;
		long d = t / 24;
		boolean i = false;
		
		StringBuffer sb = new StringBuffer();
		if (d > 0) {
			sb.append(d + " Day");
			if (d != 1)
				sb.append("s");
			
			i = true;
		}
		
		if (h > 0) {
			if (i)
				sb.append(" ");
			
			sb.append(h + " Hour");
			if (h != 1)
				sb.append("s");
			
			i = true;
		}
		
		if (m > 0) {
			if (i)
				sb.append(" ");
			
			sb.append(m + " Minute");
			if (m != 1)
				sb.append("s");
			
			i = true;
		}
		
		if (s > 0) {
			if (i)
				sb.append(" ");
			
			sb.append(s + " Second");
			if (s != 1)
				sb.append("s");
			
			i = true;
		}
		
		if (ms > 0) {
			if (i)
				sb.append(" ");
			
			sb.append(ms + " Millisecond");
			if (ms != 1)
				sb.append("s");
		}
		
		return sb.toString();
	}
		
	public void setMilliseconds(long milliseconds) {
		this.milliseconds = milliseconds;
	}
	

	public Integer getCursor() {
		return cursor;
	}
	
	public void setCursor(Integer cursor) {
		this.cursor = cursor;
	}
	
	public void setCursor(String cursor) {
		try {
			this.cursor = Integer.parseInt(cursor);
		} catch(Exception e) {
			++this.cursor;
		}
	}
	
	public void incCursor() {
		++cursor;
	}
	
	public Integer getSize() {
		return size;
	}
	
	public void setSize(Integer size) {
		this.size = size;
	}
	
	public void setSize(String size) {
		try {
			this.size = Integer.parseInt(size);
		} catch(Exception e) {
			this.size = 0;
		}
	}
	
	public void dumpToken(PrintStream out) {
		out.print("ResumptionToken Detected.");
		if (cursor > 0)
			out.print(" Cursor: " + cursor);
		
		if (size > 0)
			out.print(" Size: " + size);
		
		out.println();
	}

	@Override
	public String toString() {
		return "SetStatus [name=" + name + ", title=" + title + ", error="
				+ error + ", token=" + token + ", cursor=" + cursor + ", size="
				+ size + ", files=" + files + ", records=" + records
				+ ", milliseconds=" + milliseconds + "]";
	}
}
