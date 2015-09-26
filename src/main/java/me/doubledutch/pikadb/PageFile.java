package me.doubledutch.pikadb;

import java.io.*;
import java.nio.channels.*;
import java.util.*;

public class PageFile{
	private Map<Integer,Page> pageMap=new HashMap<Integer,Page>();
	private RandomAccessFile pageFile;
	private FileChannel pageFileChannel;
	private String filename;
	private WriteAheadLog wal=null;
	public static int LRU_SIZE = 10;
	private Deque<Page> pageLRU;
	private Deque<Page> pageSecondLRU;

	public PageFile(String filename) throws IOException{
		this.filename=filename;
		pageFile=new RandomAccessFile(filename,"rw");
		pageFileChannel=pageFile.getChannel();
		File ftest=new File(filename+".wal");
		if(ftest.exists()){
			wal=new WriteAheadLog(filename+".wal");
			recoverTransaction();
			ftest.delete();
			wal=null;
		}
		this.pageLRU = new LinkedList<Page>();
		this.pageSecondLRU = new LinkedList<Page>();
	}

	private void recoverTransaction(){
		// Replay everything in the write ahead log
	}

	protected void trimPageSet(int id){
		if (!pageMap.containsKey(id)) {
			return;
		}

		// -- SLRU --
		// If the page exists in L1
		// If L2 at size, move end to back of L2
		// Else, move it to L2
		Page page=pageMap.get(id);
		if (pageLRU.contains(page)) {
			pageLRU.remove(page);
			pageSecondLRU.addLast(page);
			//System.out.println("L2 addition");
			if (pageSecondLRU.size() > LRU_SIZE) {
				Page toOne=pageSecondLRU.removeLast();
				pageLRU.addFirst(toOne);
				//System.out.println("L2 eviction");
			}
			return;
		}
		// If page exists in L2, move to L2 front
		if (pageSecondLRU.contains(page)) {
			//System.out.println("L2 update");
			pageSecondLRU.remove(page);
			pageSecondLRU.addFirst(page);
			return;
		}
		// If L1 size is maximum, move back to L2 front
		// Put at front of L1 cache
		//System.out.println("L1 addition");
		pageLRU.addFirst(page);
		if (pageLRU.size() > LRU_SIZE) {
			//System.out.println("L1 eviction");
			Page evict=pageLRU.removeLast();
			evict.unloadRawData();
		}

		//

		/* -- LRU --
		if (pageLRU.size() >= LRU_SIZE) {
			Page evict=pageLRU.removeLast();
			evict.unloadRawData();
		}
		Page page=pageMap.get(id);
		if (pageLRU.contains(page)) {
			pageLRU.remove(page);
		}
		pageLRU.add(page);
		*/
	}

	public Page getPage(int id) throws IOException{
		// Call a method to trim in memory pages
		trimPageSet(id);
		if(pageMap.containsKey(id)){
			return pageMap.get(id);
		}
		Page page=new Page(id,id*Page.SIZE,pageFile,this);
		pageMap.put(id,page);
		return page;
	}

	public int getPageCount() throws IOException{
		return (int)(pageFile.length()/Page.SIZE);
	}

	public Page createPage() throws IOException{
		int id=getPageCount();
		// System.out.println("Create page "+id + " at "+pageFile.length());
		pageFile.setLength(pageFile.length()+Page.SIZE);
		Page page=getPage(id);
		page.setNextPageId(-1);
		page.saveMetaData();
		// page.saveChanges();
		pageFileChannel.force(true);
		return getPage(id);
	}

	public void saveChanges(boolean sort) throws IOException{
		// Write to write ahead log
		if(wal==null){
			wal=new WriteAheadLog(filename);
		}
		wal.beginTransaction();
		for(Page page:pageMap.values()){
			if(page.isDirty() && page.getFillRatio()>0.5  && sort){ // TODO: figure out the right threshold
				page.flatten();
				Column.sort(page);
			}
			page.commitChanges(wal);
		}
		wal.closeTransaction();
		// Write to page file
		for(Page page:pageMap.values()){
			page.saveChanges();
		}
		pageFileChannel.force(true);
		wal.commitTransaction();
	}

	public void close() throws IOException{
		pageFile.close();
	}
}