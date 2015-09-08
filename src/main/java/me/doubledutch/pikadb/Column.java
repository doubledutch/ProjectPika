package me.doubledutch.pikadb;

import java.util.*;
import java.io.*;

public class Column{
	private PageFile pageFile;
	public int rootId;
	private int knownFreePageId;

	public Column(PageFile pageFile,int rootId){
		this.pageFile=pageFile;
		this.rootId=rootId;
		knownFreePageId=rootId;
	}

	public void delete(int oid) throws IOException{
		ObjectSet set=new ObjectSet(false);
		set.addOID(oid);
		Page page=pageFile.getPage(rootId);
		while(page!=null){
			// Delete values
			Variant.deleteValues(set,page);
			// Find next page
			int nextPageId=page.getNextPageId();
			if(nextPageId==-1){
				page=null;
			}else{
				page=pageFile.getPage(nextPageId);
			}
		}
	}

	public void append(Variant v) throws IOException{
		Page page=getFirstFit(v.getSize());
		page.appendData(v.toByteArray());
		page.addToBloomFilter(v.getOID());
	}

	private Page getFirstFit(int size) throws IOException{
		Page page=pageFile.getPage(knownFreePageId);
		while(page.getFreeSpace()<size){
			int nextPageId=page.getNextPageId();
			if(nextPageId==-1){
				Page next=pageFile.createPage();
				// System.out.println("next: "+next.getId());
				page.setNextPageId(next.getId());
				page=next;
			}else{
				page=pageFile.getPage(nextPageId);
			}
		}
		knownFreePageId=page.getId();
		return page;
	}

	private List<Variant> scan(Page page,ObjectSet set) throws IOException{
		// System.out.println("scan("+page.getId()+")");
		List<Variant> list=new ArrayList<Variant>();
		DataInput in=page.getDataInput();
		Variant v=Variant.readVariant(in,set);
		while(v!=null){
			if(v.getType()!=Variant.SKIP){
				list.add(v);
				if(!set.isOpen()){
					if(set.getMatchCount()==set.getCount()){
						return list;
					}
				}
			}
			v=Variant.readVariant(in,set);
		}
		return list;
	}

	public List<Variant> scan(ObjectSet set) throws IOException{
		set.resetMatchCounter();
		// System.out.println("starting col scan at "+rootId);
		List<Variant> list=new ArrayList<Variant>();
		Page page=pageFile.getPage(rootId);
		// System.out.println("Got page "+page.getId());
		while(page!=null){
			if((!set.isOpen()) && set.anyObjectsInBloomFilter(page.getBloomFilter())){
				list.addAll(scan(page,set));
			}else if(set.isOpen()){
				list.addAll(scan(page,set));
			}
			if(!set.isOpen()){
				if(set.getMatchCount()==set.getCount()){
					return list;
				}
			}
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