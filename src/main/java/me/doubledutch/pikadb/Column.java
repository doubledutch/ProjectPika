package me.doubledutch.pikadb;

import java.util.*;
import java.io.*;

public class Column{
	private PageFile pageFile;
	private int rootId;
	private int knownFreePageId;

	public Column(PageFile pageFile,int rootId){
		this.pageFile=pageFile;
		this.rootId=rootId;
		knownFreePageId=rootId;
	}

	public void append(Variant v) throws IOException{
		Page page=getFirstFit(v.getSize());
		page.appendData(v.toByteArray());
	}

	private Page getFirstFit(int size) throws IOException{
		Page page=pageFile.getPage(knownFreePageId);
		while(page.getFreeSpace()<size){
			int nextPageId=page.getNextPageId();
			if(nextPageId==-1){
				Page next=pageFile.createPage();
				page.setNextPageId(next.getId());
				page=next;
			}else{
				page=pageFile.getPage(nextPageId);
			}
		}
		knownFreePageId=page.getId();
		return page;
	}

	private List<Variant> scan(Page page) throws IOException{
		// System.out.println("scan("+page.getId()+")");
		List<Variant> list=new ArrayList<Variant>();
		DataInput in=page.getDataInput();
		Variant v=Variant.readVariant(in);
		while(v!=null){
			list.add(v);
			v=Variant.readVariant(in);
		}
		return list;
	}

	public List<Variant> scan() throws IOException{
		List<Variant> list=new ArrayList<Variant>();
		Page page=pageFile.getPage(rootId);
		while(page!=null){
			list.addAll(scan(page));
			int next=page.getNextPageId();
			// System.out.println("next:"+next);
			if(next>-1){
				page=pageFile.getPage(next);
			}else{
				page=null;
			}
		}
		return list;
	}
}