package me.doubledutch.pikadb;

import java.io.*;
import java.nio.channels.*;
import java.util.*;

public class PageFile{
	private Map<Integer,Page> pageMap=new HashMap<Integer,Page>();
	private RandomAccessFile pageFile;
	private FileChannel pageFileChannel;

	public PageFile(String filename) throws IOException{
		pageFile=new RandomAccessFile(filename,"rw");
		pageFileChannel=pageFile.getChannel();
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
		// System.out.println("Create page "+id);
		pageFile.setLength(pageFile.length()+Page.SIZE);
		Page page=getPage(id);
		page.setNextPageId(-1);
		page.saveMetaData();
		// page.saveChanges();
		pageFileChannel.force(true);
		return getPage(id);
	}

	public void saveChanges() throws IOException{
		for(Page page:pageMap.values()){
			page.saveChanges();
		}
		pageFileChannel.force(true);
	}

	public void close() throws IOException{
		pageFile.close();
	}
}