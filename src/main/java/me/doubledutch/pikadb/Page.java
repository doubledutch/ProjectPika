package me.doubledutch.pikadb;

import java.util.*;
import java.io.*;

public class Page{
	public final static int HEADER=4+4;
	public final static int PAYLOAD=8192-HEADER-2;
	public final static int SIZE=PAYLOAD+HEADER+2;
	// TODO: add stop byte after data payload when creating and updating the file

	private int id;
	private long offset;
	private RandomAccessFile pageFile;
	private byte[] rawData;
	private int currentFill=0;
	private int nextPageId=-1;

	private boolean dirty=false;

	private List<PageDiff> diffList=new LinkedList<PageDiff>();

	public Page(int id,long offset,RandomAccessFile pageFile) throws IOException{
		this.id=id;
		this.offset=offset;
		this.pageFile=pageFile;

		loadMetaData();
		rawData=new byte[PAYLOAD];
		pageFile.seek(offset+HEADER);
		pageFile.readFully(rawData);
		// System.out.println("Page opened: "+id+" nextPageId: "+nextPageId+" currentFill: "+currentFill);
	}

	public void saveMetaData() throws IOException{
		pageFile.seek(offset);
		pageFile.writeInt(nextPageId);
		pageFile.writeInt(currentFill);
	}

	public void loadMetaData() throws IOException{
		pageFile.seek(offset);
		nextPageId=pageFile.readInt();
		currentFill=pageFile.readInt();
	}

	public int getCurrentFill(){
		return currentFill;
	}

	public int getNextPageId(){
		return nextPageId;
	}

	public void setNextPageId(int next){
		nextPageId=next;
	}

	public int getId(){
		return id;
	}

	public int getFreeSpace(){
		return PAYLOAD-currentFill;
	}

	public void commitChanges(WriteAheadLog wal) throws IOException{
		if(!dirty){
			return;
		}
		for(PageDiff diff:diffList){
			wal.addPageDiff(getId(),diff);
		}
		wal.addMetaData(getId(),getNextPageId(),getCurrentFill());
	}

	public void saveChanges() throws IOException{
		if(!dirty){
			return;
		}
		// System.out.println("Page.SaveChanges");
		for(PageDiff diff:diffList){
			System.arraycopy(diff.getData(),
                        	0,
                             rawData,
                             diff.getOffset(),
                             diff.getData().length);
		}
		pageFile.seek(offset+HEADER);
		pageFile.write(rawData);
		/*
		Old diff by diff save
		for(PageDiff diff:diffList){
			pageFile.seek(offset+HEADER+diff.getOffset());
			pageFile.write(diff.getData());
		}
		*/
		saveMetaData();
		dirty=false;
	}

	public DataInput getDataInput() throws IOException{
		return new DataInputStream(new ByteArrayInputStream(rawData));
	}

	public void addDiff(int offset,byte[] data){
		diffList.add(new PageDiff(offset,data));
		dirty=true;
	}

	public void appendData(byte[] data){
		diffList.add(new PageDiff(currentFill,data));
		currentFill+=data.length;
		dirty=true;
		// System.out.println("currentFill: "+id+" "+currentFill+" "+data.length);
	}
}