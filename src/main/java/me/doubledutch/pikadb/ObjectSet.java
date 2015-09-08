package me.doubledutch.pikadb;

import java.util.*;
import org.json.*;

public class ObjectSet{
	private Map<Integer,JSONObject> objectMap=new HashMap<Integer,JSONObject>();
	private List<JSONObject> objectList=new ArrayList<JSONObject>();
	private boolean open;

	public ObjectSet(boolean open){
		this.open=open;
	}

	public boolean contains(int oid){
		if(open){
			return true;
		}
		return objectMap.containsKey(oid);
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