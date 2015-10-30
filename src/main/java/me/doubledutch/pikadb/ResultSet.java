package me.doubledutch.pikadb;

import java.util.*;
import org.json.*;

public class ResultSet{
	private long tStart,tEnd;
	private JSONArray executionPlan=new JSONArray();

	private List<JSONObject> data=null;
	private int pointer=-1;
	private JSONObject currentObject=null;

	public ResultSet(){

	}

	public void startTimer(){
		tStart=System.nanoTime();
	}

	public void endTimer(){
		tEnd=System.nanoTime();
	}

	public void setObjectList(List<JSONObject> data){
		this.data=data;
	}

	public List<JSONObject> getObjectList(){
		return data;
	}

	public void addExecutionPlan(JSONObject obj){
		executionPlan.put(obj);
	}

	public JSONObject getExecutionPlan() throws JSONException{
		JSONObject obj=new JSONObject();
		obj.put("time",(tEnd-tStart));
		obj.put("steps",executionPlan);
		return obj;
	}
}