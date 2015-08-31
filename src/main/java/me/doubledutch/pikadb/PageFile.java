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
		Page page=new Page(id,id*Page.SIZE,pageFile);
		return page;
	}

	public int getPageCount() throws IOException{
		return (int)(pageFile.length()/Page.SIZE);
	}

	public Page createPage() throws IOException{
		int id=getPageCount();
		pageFile.setLength(pageFile.length()+Page.SIZE);
		return getPage(id);
	}

	public void saveChanges() throws IOException{
		for(Page page:pageList){
			page.saveChanges();
		}
	}
}