package me.doubledutch.pikadb;

import java.util.*;
import org.json.*;

public class ObjectSet{
	private Map<String,Map<Integer,Variant>> columnValueMap=new HashMap<String,Map<Integer,Variant>>();
	private Set<Integer> oidSet=new LinkedHashSet<Integer>();
	// TODO: Check performance implications of using linked hash set

	private boolean open;
	private int matchCounter;

	public ObjectSet(boolean open){
		this.open=open;
	}

	public boolean anyObjectsInBloomFilter(LargeHash bloomfilter){
		// TODO: look into maintaining a separate set of hashed oid's
		for(int oid:oidSet){
			LargeHash hoid=MurmurHash3.getSelectiveBits(oid);
			// if((hoid & bloomfilter) == hoid){
			if(bloomfilter.containsHash(hoid)){
				return true;
			}
		}
		return false;
	}

	public boolean isOpen(){
		return open;
	}

	public int getCount(){
		return oidSet.size();
	}

	public int getMatchCount(){
		return matchCounter;
	}

	public void resetMatchCounter(){
		matchCounter=0;
	}

	public boolean contains(Integer oid){
		if(open){
			return true;
		}
		if(oidSet.contains(oid)){
			matchCounter++;
			return true;
		}
		return false;
	}

	public void addOID(Integer oid){
		// JSONObject obj=new JSONObject();
		// objectMap.put(oid,obj)
		// if(!oidSet.contains(oid)){
			oidSet.add(oid);
		// }
		// objectList.add(obj);
	}


	public JSONObject getObject(Integer oid)throws JSONException{
		if(!oidSet.contains(oid)){
			return null;
		}
		JSONObject obj=new JSONObject();
		for(String key:columnValueMap.keySet()){
			Map<Integer,Variant> map=columnValueMap.get(key);
			if(map.containsKey(oid)){
				Variant v=map.get(oid);
				obj.put(key,v.getObjectValue());
			}
		}
		return obj;
		// return objectMap.get(oid);
	}

	public void addVariantList(String columnName,List<Variant> list){
		if(!columnValueMap.containsKey(columnName)){
			columnValueMap.put(columnName,new HashMap<Integer,Variant>());
		}
		Map<Integer,Variant> map=columnValueMap.get(columnName);
		for(Variant v:list){
			Integer oid=v.getOID();
			if(!oidSet.contains(oid)){
				if(!open){
					continue;
				}else{
					addOID(oid);
					map.put(oid,v);
				}
			}else{
				map.put(oid,v);
			}
		}
	}

	public void addVariant(String columnName,Variant v){ 
		Integer oid=v.getOID();
		if(!oidSet.contains(oid)){
			if(!open){
				return;
			}
			addOID(oid);
		}
		if(!columnValueMap.containsKey(columnName)){
			columnValueMap.put(columnName,new HashMap<Integer,Variant>());
		}
		columnValueMap.get(columnName).put(oid,v);
		/*if(!objectMap.containsKey(oid)){
			if(!open){
				return;
			}
			addOID(oid);
		}
		JSONObject obj=objectMap.get(oid);
		obj.put(columnName,v.getObjectValue());*/
	}

	public Collection<Variant> getVariants(String columnName){
		if(columnValueMap.containsKey(columnName)){
			return columnValueMap.get(columnName).values();
		}
		return new LinkedList<Variant>();
	}

	public List<JSONObject> getObjectList()throws JSONException{
		// System.out.println("Crafting objects");
		List<JSONObject> objectList=new ArrayList<JSONObject>();
		for(Integer oid:oidSet){
			// System.out.println("Making one");
			objectList.add(getObject(oid));
		}
		return objectList;
	}
}