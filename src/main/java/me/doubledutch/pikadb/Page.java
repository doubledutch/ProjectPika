package me.doubledutch.pikadb;

import java.io.*;

public class Page{
	public static int SIZE=8192;

	private int id;
	private long offset;
	private RandomAccessFile pageFile;
	private byte[] rawData;

	public Page(int id,long offset,RandomAccessFile pageFile) throws IOException{
		this.id=id;
		this.offset=offset;
		this.pageFile=pageFile;

		rawData=new byte[SIZE];
		pageFile.seek(offset);
		pageFile.readFully(rawData);
	}

	public void saveChanges() throws IOException{
		pageFile.seek(offset);
		pageFile.write(rawData);
	}
}