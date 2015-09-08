package me.doubledutch.pikadb;

import java.util.*;
import org.json.*;

public class ObjectSet{
	private Map<Integer,JSONObject> objectMap=new HashMap<Integer,JSONObject>();
	private List<JSONObject> objectList=new ArrayList<JSONObject>();
	private boolean open;
	private int matchCounter;


	public ObjectSet(boolean open){
		this.open=open;
	}

	public boolean anyObjectsInBloomFilter(int bloomfilter){
		for(int oid:objectMap.keySet()){
			if((oid & bloomfilter) == oid){
				return true;
			}
		}
		return false;
	}

	public boolean isOpen(){
		return open;
	}

	public int getCount(){
		return objectList.size();
	}

	public int getMatchCount(){
		return matchCounter;
	}

	public void resetMatchCounter(){
		matchCounter=0;
	}

	public boolean contains(int oid){
		if(open){
			return true;
		}
		if(objectMap.containsKey(oid)){
			matchCounter++;
			return true;
		}
		return false;
	}

	public void addOID(int oid){
		JSONObject obj=new JSONObject();
		objectMap.put(oid,obj);
		objectList.add(obj);
	}

	public JSONObject getObject(int oid){
		return objectMap.get(oid);
	}

	public void addVariant(String columnName,Variant v) throws JSONException{ 
		int oid=v.getOID();
		if(!objectMap.containsKey(oid)){
			if(!open){
				return;
			}
			addOID(oid);
		}
		JSONObject obj=objectMap.get(oid);
		obj.put(columnName,v.getObjectValue());
	}

	public List<JSONObject> getObjectList(){
		return objectList;
	}
}