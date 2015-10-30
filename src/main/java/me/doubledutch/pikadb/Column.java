package me.doubledutch.pikadb;

import java.util.*;
import java.io.*;

public class Column{
	private PageFile pageFile;
	public int rootId;
	private int knownFreePageId;
	private boolean sortable;
	private String name;

	public Column(String name,PageFile pageFile,int rootId,boolean sortable){
		this.name=name;
		this.pageFile=pageFile;
		this.rootId=rootId;
		knownFreePageId=rootId;
		this.sortable=sortable;
	}

	public void delete(int oid) throws IOException{
		ObjectSet set=new ObjectSet(false);
		set.addOID(oid);
		Page page=pageFile.getPage(rootId);
		while(page!=null){
			if(page.isInBloomFilter(oid)){
				// Delete values
				Variant.deleteValues(set,page);
			}
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
		while(page.isSorted() || page.getFreeSpace()<size){
			int nextPageId=page.getNextPageId();
			if(nextPageId==-1){
				Page next=pageFile.createPage();
				if(!sortable){
					next.makeUnsortable();
				}
				page.setNextPageId(next.getId());
				page=next;
			}else{
				page=pageFile.getPage(nextPageId);
			}
		}
		knownFreePageId=page.getId();
		return page;
	}

	protected static void scan(ColumnResult result,Page page,ObjectSet set) throws IOException{
		result.incPageScanned();
		DataInput in=page.getDataInput();
		Variant v=Variant.readVariant(in,set);
		while(v!=null){
			result.incVariantRead();
			if(v.getType()!=Variant.SKIP){
				result.add(v);
				if(!set.isOpen()){
					if(set.getMatchCount()==set.getCount()){
						return;
					}
				}
			}
			v=Variant.readVariant(in,set);
		}
		return;
	}

	public ColumnResult scan(ObjectSet set) throws IOException{
		set.resetMatchCounter();
		ColumnResult result=new ColumnResult(name);
		result.startTimer();
		Page page=pageFile.getPage(rootId);
		while(page!=null){
			if((!set.isOpen()) && set.anyObjectsInBloomFilter(page.getBloomFilter())){
				scan(result,page,set);
			}else if(set.isOpen()){
				scan(result,page,set);
			}else{
				result.incPageSkipped();
			}
			if(!set.isOpen()){
				if(set.getMatchCount()==set.getCount()){
					result.endTimer();
					return result;
				}
			}
			int next=page.getNextPageId();
			if(next>-1){
				page=pageFile.getPage(next);
			}else{
				page=null;
			}
		}
		result.endTimer();
		return result;
	}

	public static void sort(Page page) throws IOException{
		ObjectSet set=new ObjectSet(true);
		ColumnResult result=new ColumnResult("");
		scan(result,page,set);
		List<Variant> list=result.getVariantList();	
		Collections.sort(list);	
		Variant last=list.remove(list.size()-1);
		list.add(0,last);
		int offset=0;
		for(Variant v:list){
			page.addDiff(offset,v.toByteArray());
			offset+=v.getSize();
		}
		page.setSorted(true);
	}
}