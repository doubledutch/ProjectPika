package me.doubledutch.pikadb;

import java.io.*;
import java.util.*;
import org.json.*;

import me.doubledutch.pikadb.query.*;

public class Table{
	private String name;
	private PageFile pageFile;
	private int rootPageId;
	private Column metaData;
	private Map<String,Column> columnMap;
	private boolean preserve_order;

	public Table(String name,PageFile pageFile,int rootPageId,boolean preserve_order) throws IOException{
		this.name=name;
		this.pageFile=pageFile;
		this.rootPageId=rootPageId;
		this.preserve_order=preserve_order;
		metaData=new Column(name+".metadata",pageFile,rootPageId,false);
		loadColumns();
	}

	protected void enforceConstraints(int... constraints) throws IOException{
		// TODO: Implement
	}

	private void loadColumns() throws IOException{
		ObjectSet set=new ObjectSet(true);
		Map<String,Column> tmp=new HashMap<String,Column>();
		ColumnResult result=metaData.scan(set);
		List<Variant> list=result.getVariantList();
		int index=0;
		while(list.size()>index){
			Variant.String name=(Variant.String)list.get(index++);
			Variant.Integer pageId=(Variant.Integer)list.get(index++);
			Column col=new Column(name.getValue(),pageFile,pageId.getValue(),!preserve_order);
			tmp.put(name.getValue(),col);
		}
		columnMap=tmp;
	}

	public void declareColumn(String name) throws IOException{

	}

	public void declareColumn(String name,int... constraints) throws IOException{
		
	}

	public String[] getColumns(){
		return columnMap.keySet().toArray(new String[0]);
	}

	private Column getColumn(String name) throws IOException{
		if(!columnMap.containsKey(name)){
			return createColumn(name);
		}
		return columnMap.get(name);
	}

	private Column createColumn(String name) throws IOException{
		Page page=pageFile.createPage();
		metaData.append(new Variant.String(-1,name));
		metaData.append(new Variant.Integer(-1,page.getId()));
		pageFile.saveChanges();
		Column col=new Column(name,pageFile,page.getId(),!preserve_order);
		columnMap.put(name,col);
		return col;
	}

	public void delete(int oid) throws IOException{
		for(Column col:columnMap.values()){
			col.delete(oid);
		}
	}

	public void update(int oid,JSONObject obj) throws IOException,JSONException{
		Iterator<String> it=obj.keys();

        while(it.hasNext()){
            String key=it.next();
            Column col=getColumn(key);
            Object value=obj.get(key);
            Variant variant=Variant.createVariant(oid,value);
            col.delete(oid);
            col.append(variant);
        }
	}

	public void add(int oid,JSONObject obj) throws IOException,JSONException{
		Iterator<String> it=obj.keys();
        while(it.hasNext()){
            String key=it.next();
            Column col=getColumn(key);
            Object value=obj.get(key);
            Variant variant=Variant.createVariant(oid,value);
            col.append(variant);
        }
	}

	public Query select(String... columns){
		Query q=new Query(this,columns);
		return q;
	}

	public ResultSet scan() throws IOException,JSONException{
		return scan(columnMap.keySet().toArray(new String[0]));
	}

	public ResultSet scan(String[] columns) throws IOException,JSONException{
		ObjectSet set=new ObjectSet(true);
		return scan(set,columns,null);
	}

	public ResultSet scan(int oid)  throws IOException,JSONException{
		return scan(oid,columnMap.keySet().toArray(new String[0]));
	}

	public ResultSet scan(int oid,String[] columns)  throws IOException,JSONException{
		ObjectSet set=new ObjectSet(false);
        set.addOID(oid);
        return scan(set,columns,null);
        /*JSONObject obj=set.getObject(oid);
        Iterator<String> it=obj.keys();
        if(!it.hasNext()){
            return null;
        }*/
	}

	public ResultSet scan(ObjectSet set) throws IOException,JSONException{
		return scan(set,columnMap.keySet().toArray(new String[0]),null);
	}

	// TODO: change to columnscan with predicate!!!

	public ResultSet scan(ObjectSet set,String[] columns,Predicate predicate) throws IOException,JSONException{
		String operation="table.scan";
		if(!set.isOpen()){
			operation="table.seek";
		}
		ResultSet result=new ResultSet(operation);
		result.startTimer();
		for(String columnName:columns){
			Column col=columnMap.get(columnName);
			ColumnResult colResult=col.scan(set,predicate);
			List<Variant> list=colResult.getVariantList();

			set.addVariantList(columnName,list);
			// for(Variant v:list){
			//	set.addVariant(columnName,v);
			// }
			result.addExecutionPlan(colResult.getExecutionPlan());
		}

		// result.setObjectList(set.getObjectList());
		result.setObjectSet(set);
		result.endTimer();
		return result;
	}
}