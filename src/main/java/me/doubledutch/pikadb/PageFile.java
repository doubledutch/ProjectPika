package me.doubledutch.pikadb;

import java.io.*;
import java.nio.channels.*;
import java.util.*;

public class PageFile{
	private List<Page> pageList=new ArrayList<Page>();
	private RandomAccessFile pageFile;
	private FileChannel pageFileChannel;

	public PageFile(String filename) throws IOException{
		pageFile=new RandomAccessFile(filename,"rw");
		pageFileChannel=pageFile.getChannel();
	}

	public Page getPage(int id) throws IOException{
		if(pageList.size()>id){
			return pageList.get(id);
		}
		Page page=new Page(id,id*Page.SIZE,pageFile);
		pageList.add(page);
		return page;
	}

	public int getPageCount() throws IOException{
		return (int)(pageFile.length()/Page.SIZE);
	}

	public Page createPage() throws IOException{
		// System.out.println("Create page");
		int id=getPageCount();
		pageFile.setLength(pageFile.length()+Page.SIZE);
		Page page=getPage(id);
		page.setNextPageId(-1);
		page.saveMetaData();
		return page;
	}

	public void saveChanges() throws IOException{
		for(Page page:pageList){
			page.saveChanges();
		}
		pageFileChannel.force(true);
	}

	public void close() throws IOException{
		pageFile.close();
	}
}