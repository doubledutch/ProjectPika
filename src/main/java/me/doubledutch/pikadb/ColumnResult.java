package me.doubledutch.pikadb;

import java.util.*;
import org.json.*;

public class ColumnResult{
	private List<Variant> list;
	private long tStart,tEnd;
	private int pagesScanned,pagesSkipped,variantsRead;
	private String name;
	private String type;

	public ColumnResult(String name,String type){
		this.name=name;
		this.type=type;
		this.list=new ArrayList<Variant>();
	}

	public void add(Variant v){
		list.add(v);
	}

	public List<Variant> getVariantList(){
		return list;
	}

	public void startTimer(){
		tStart=System.nanoTime();
	}

	public void endTimer(){
		tEnd=System.nanoTime();
	}

	public void incPageScanned(){
		pagesScanned++;
	}

	public void incPageSkipped(){
		pagesSkipped++;
	}

	public void incVariantRead(){
		variantsRead++;
	}

	public JSONObject getExecutionPlan() throws JSONException{
		JSONObject obj=new JSONObject();
		obj.put("column",name);
		obj.put("operation",type);
		obj.put("time",(tEnd-tStart));
		obj.put("pages.scanned",pagesScanned);
		obj.put("pages.skipped",pagesSkipped);
		obj.put("variants.read",variantsRead);
		return obj;
	}
}