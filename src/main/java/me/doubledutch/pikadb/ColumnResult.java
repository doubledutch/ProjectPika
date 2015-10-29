package me.doubledutch.pikadb;

import java.util.*;

public class ColumnResult{
	private List<Variant> list;
	private long tStart,tEnd;
	private int pagesScanned,pagesSkipped,variantsRead;

	public ColumnResult(){
		this.list=new ArrayList<Variant>();
	}

	public void add(Variant v){
		list.add(v);
	}

	public void startTimer(){
		tStart=System.nanoTime();
	}

	public void endTimer(){
		tEnd=System.nanoTime();
	}

	public void incPageScaned(){
		pagesScanned++;
	}

	public void incPageSkipped(){
		pagesSkipped++;
	}

	public void incVariantRead(){
		variantsRead++;
	}

	public JSONObject getExecutionPlan(){
		JSONObject obj=new JSONObject();
		obj.put("time",(tEnd-tStart));
		obj.put("pages.scanned",pagesScanned);
		obj.put("pages.skipped",pagesSkipped);
		obj.put("variants.read",variantsRead);
		return obj;
	}
}