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
	}

	private void recoverTransaction(){
		// Replay everything in the write ahead log
	}

	public Page getPage(int id) throws IOException{
		if(pageMap.containsKey(id)){
			return pageMap.get(id);
		}
		Page page=new Page(id,id*Page.SIZE,pageFile);
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

	public void saveChanges() throws IOException{
		// Write to write ahead log
		if(wal==null){
			wal=new WriteAheadLog(filename);
		}
		wal.beginTransaction();
		for(Page page:pageMap.values()){
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