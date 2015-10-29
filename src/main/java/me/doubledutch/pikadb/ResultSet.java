package me.doubledutch.pikadb;

import java.util.*;
import org.json.*;

public class ResultSet{
	private long executionTime=-1;
	private JSONArray executionPlan=new JSONArray();

	private List<JSONObject> data=null;
	private int pointer=-1;
	private JSONObject currentObject=null;

	public ResultSet(){

	}

	public void addExecutionPlan(JSONObject obj){
		executionPlan.put(obj);
	}
}