package me.doubledutch.pikadb;

import java.util.*;
import java.io.*;

public class Page{
	public static int SIZE=8192;

	private int id;
	private long offset;
	private RandomAccessFile pageFile;
	private byte[] rawData;

	private int currentFill=0;

	private List<PageDiff> diffList=new LinkedList<PageDiff>();

	public Page(int id,long offset,RandomAccessFile pageFile) throws IOException{
		this.id=id;
		this.offset=offset;
		this.pageFile=pageFile;

		rawData=new byte[SIZE];
		pageFile.seek(offset);
		pageFile.readFully(rawData);
	}

	public void saveChanges() throws IOException{
		System.out.println("Page.SaveChanges");
		for(PageDiff diff:diffList){
			pageFile.seek(offset+diff.getOffset());
			pageFile.write(diff.getData());
		}
	}

	public DataInput getDataInput() throws IOException{
		return new DataInputStream(new ByteArrayInputStream(rawData));
	}

	public void addDiff(int offset,byte[] data){
		diffList.add(new PageDiff(offset,data));
	}

	public void appendData(byte[] data){
		diffList.add(new PageDiff(currentFill,data));
		currentFill+=data.length;
	}
}