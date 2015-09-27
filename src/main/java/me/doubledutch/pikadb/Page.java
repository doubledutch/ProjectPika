package me.doubledutch.pikadb;

import java.util.*;
import java.io.*;

public class Page{
	public static int UNSORTED=0;
	public static int SORTED=1;
	public static int UNSORTABLE=2;

	public final static int HEADER=4+4+4+1; // nextPageId + currentFill + bloomFilter + type
	public final static int PAYLOAD=4096-HEADER-16; 
	public final static int SIZE=PAYLOAD+HEADER+16;
	// TODO: add stop byte after data payload when creating and updating the file
	// TODO: fix the EOF errors requiring -16

	private int id;
	private long offset;
	private RandomAccessFile pageFile;
	private byte[] rawData;
	private int currentFill=0;
	private int nextPageId=-1;
	private int bloomfilter=0;
	private int type=UNSORTED;

	private PageFile pageHandler;

	private boolean dirty=false;

	private List<PageDiff> diffList=new LinkedList<PageDiff>();

	protected int cacheMiss, cacheHit;

	public Page(int id,long offset,RandomAccessFile pageFile,PageFile pageHandler) throws IOException{
		this.id=id;
		this.offset=offset;
		this.pageFile=pageFile;
		this.pageHandler=pageHandler;

		loadMetaData();
		rawData=null;
		// rawData=new byte[PAYLOAD];
		// pageFile.seek(offset+HEADER);
		// pageFile.readFully(rawData);
		// System.out.println("Page opened: "+id+" nextPageId: "+nextPageId+" currentFill: "+currentFill);
	}

	public boolean isDirty(){
		return dirty;
	}

	public float getFillRatio(){
		return currentFill/(float)PAYLOAD;
	}

	public boolean isSorted(){
		return type==SORTED;
	}

	public void setSorted(boolean val){
		if(val){
			type=SORTED;
		}else{
			type=UNSORTED;
		}
	}

	public void makeUnsortable(){
		type=UNSORTABLE;
	}

	protected void unloadRawData(){
		rawData=null;
	}

	private void loadRawData() throws IOException{
		if(rawData==null){
			pageHandler.trimPageSet(id);
			cacheMiss++;
			rawData=new byte[PAYLOAD];
			pageFile.seek(offset+HEADER);
			pageFile.readFully(rawData);
		} else {
			cacheHit++;
		}
	}

	public void saveMetaData() throws IOException{
		pageFile.seek(offset);
		pageFile.writeInt(nextPageId);
		pageFile.writeInt(currentFill);
		pageFile.writeInt(bloomfilter);
		pageFile.writeByte(type);
	}

	public void loadMetaData() throws IOException{
		pageFile.seek(offset);
		nextPageId=pageFile.readInt();
		currentFill=pageFile.readInt();
		bloomfilter=pageFile.readInt();
		type=pageFile.readByte();
	}

	public void addToBloomFilter(int oid){
		bloomfilter=bloomfilter | oid;
	}

	public boolean isInBloomFilter(int oid){
		return (bloomfilter & oid) == oid;
	}

	public int getBloomFilter(){
		return bloomfilter;
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

	public void flatten() throws IOException{
		if(!dirty){
			return;
		}
		loadRawData();
		for(PageDiff diff:diffList){
			System.arraycopy(diff.getData(),
                        	0,
                             rawData,
                             diff.getOffset(),
                             diff.getData().length);
		}
		diffList.clear();
	}

	public void saveChanges() throws IOException{
		if(!dirty){
			return;
		}
		loadRawData();
		flatten();
		pageFile.seek(offset+HEADER);
		pageFile.write(rawData);
		saveMetaData();
		dirty=false;
	}

	public DataInput getDataInput() throws IOException{
		loadRawData();
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