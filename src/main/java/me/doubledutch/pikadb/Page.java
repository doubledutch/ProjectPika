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
		pageHandler.trimPageSet(id);
		if(rawData==null){
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

/**
 * Adaptive Replacement Cache
 * Adapted from http://code.activestate.com/recipes/576532/
 */
class ARCPageCache implements PageCache{
	private int c, p;
	private Map<Integer, Page> pageMap;
	private Deque<Integer> t1, t2, b1, b2;

	public ARCPageCache(Map<Integer, Page> map, int size){
		this.c=size;
		this.p=0;
		this.pageMap=map;
		t1=new ArrayDeque<Integer>();
		t2=new ArrayDeque<Integer>();
		b1=new ArrayDeque<Integer>();
		b2=new ArrayDeque<Integer>();
	}

	public void Set(int id){
		if (t1.contains(id)){
			t1.remove(id);
			t2.addFirst(id);
			return;
		}
		if (t2.contains(id)){
			t2.remove(id);
			t2.addFirst(id);
			return;
		}
		if (b1.contains(id)){
			p = Math.min(c, p+Math.max(b2.size()/b1.size(), 1));
			replace(id);
			b1.remove(id);
			t2.addFirst(id);
			return;
		}
		if (b2.contains(id)){
			p = Math.max(0, p-Math.max(b1.size()/b2.size(),1));
			replace(id);
			b2.remove(id);
			t2.addFirst(id);
			return;
		}
		if (t1.size()+b1.size()==c){
			if (t1.size()<c){
				b1.removeLast();
				replace(id);
			} else {
				t1.removeLast();
				pageMap.get(id).unloadRawData();
			}
		} else {
			int total=t1.size()+b1.size()+t2.size()+b2.size();
			if (total >= c){
				if (total == 2*c){
					b2.removeLast();
				}
				replace(id);
			}
		}
		t1.addFirst(id);
	}

	private void replace(int id){
		Integer old;
		if (this.t1.size() > 0 && ((b2.contains(id) && t1.size() == this.p) || (t1.size() > this.p))){
			old=t1.removeLast();
			b1.addFirst(old);
		} else {
			old=t2.removeLast();
			b2.addFirst(old);
		}
		pageMap.get(old).unloadRawData();
	}
}