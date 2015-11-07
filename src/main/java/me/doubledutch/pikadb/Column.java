package me.doubledutch.pikadb;

import java.util.*;
import java.io.*;
import me.doubledutch.pikadb.query.*;

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
				page.setNextPageId(next.getId());
				page=next;
			}else{
				page=pageFile.getPage(nextPageId);

			}
		}
		knownFreePageId=page.getId();
		if(!sortable){
			page.makeUnsortable();
		}
		return page;
	}

	private static boolean addToSet(Variant v,ColumnResult result,ObjectSet set,Predicate predicate){
		boolean shouldAdd=true;
		if(predicate!=null){
			shouldAdd=predicate.testVariant(v);
		}
		if(shouldAdd){
			result.add(v);
			return true;
		}
		return false;
	}

	protected static void scan(ColumnResult result,Page page,ObjectSet set,Predicate predicate) throws IOException{
		result.incPageScanned();
		DataInput in=page.getDataInput();
		if(predicate!=null && page.isSorted()){
			int type=predicate.getType();
			if(type==Predicate.EQUALS || type==Predicate.LESSTHAN || type==Predicate.GREATERTHAN){
				Variant vMax=Variant.readVariant(in,set);
				result.incVariantRead();
				if(vMax==null || vMax.getType()==Variant.DELETE){
					return;
				}
				Variant vMin=Variant.readVariant(in,set);
				result.incVariantRead();
				if(vMin==null || vMin.getType()==Variant.DELETE){
					addToSet(vMax,result,set,predicate);
					return;
				}
				addToSet(vMax,result,set,predicate);
				addToSet(vMin,result,set,predicate);
				// System.out.println("so sorted");
			}
		}
		Variant v=Variant.readVariant(in,set);
		while(v!=null){
			result.incVariantRead();
			if(v.getType()!=Variant.SKIP){
				if(addToSet(v,result,set,predicate)){
					if(!set.isOpen()){
						if(set.getMatchCount()==set.getCount()){
							return;
						}
					}
				}
			}
			v=Variant.readVariant(in,set);
		}
		return;
	}

	public ColumnResult scan(ObjectSet set) throws IOException{
		return scan(set,null);
	}


	public ColumnResult scan(ObjectSet set,Predicate predicate) throws IOException{
		set.resetMatchCounter();
		String operation="column.scan";
		if(!set.isOpen()){
			operation="column.seek";
		}
		if(predicate!=null){
			operation="column.query";
		}
		ColumnResult result=new ColumnResult(name,operation);
		result.startTimer();
		Page page=pageFile.getPage(rootId);
		while(page!=null){
			if((!set.isOpen()) && set.anyObjectsInBloomFilter(page.getBloomFilter())){
				scan(result,page,set,predicate);
			}else if(set.isOpen()){
				scan(result,page,set,predicate);
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
		ColumnResult result=new ColumnResult("","");
		scan(result,page,set,null);
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