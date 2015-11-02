package me.doubledutch.pikadb;

import java.util.*;
import org.json.*;

public class ResultSet{
	private long tStart,tEnd;
	private String operation;
	private JSONArray executionPlan=new JSONArray();

	private ObjectSet objectSet=null;
	private List<JSONObject> data=null;
	private int pointer=-1;
	private JSONObject currentObject=null;

	public ResultSet(String operation){
		this.operation=operation;
	}

	public void startTimer(){
		tStart=System.nanoTime();
	}

	public void endTimer(){
		tEnd=System.nanoTime();
	}

	// public void setObjectList(List<JSONObject> data){
	//	this.data=data;
	// }

	public List<JSONObject> getObjectList() throws JSONException{
		if(data!=null)return data;
		long pre=System.nanoTime();
		data= objectSet.getObjectList();
		long post=System.nanoTime();
		JSONObject obj=new JSONObject();
		obj.put("operation","serialization.json");
		obj.put("time",(post-pre));
		obj.put("objects",data.size());
		executionPlan.put(obj);
		return data;
	}

	public ObjectSet getObjectSet(){
		return objectSet;
	}

	public void setObjectSet(ObjectSet set){
		this.objectSet=set;
	}

	public void addExecutionPlan(JSONObject obj){
		executionPlan.put(obj);
	}

	public JSONObject getExecutionPlan() throws JSONException{
		JSONObject obj=new JSONObject();
		obj.put("operation",operation);
		obj.put("time",(tEnd-tStart));
		obj.put("steps",executionPlan);
		return obj;
	}
}