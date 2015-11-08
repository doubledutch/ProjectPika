package me.doubledutch.pikadb.query;

import java.io.*;
import org.json.*;
import me.doubledutch.pikadb.*;
import java.util.*;

public class Query{
	private String[] columns;
	private Predicate predicate=null;
	private Table table=null;
	private ResultSet result=null;

	public Query(Table table,String... columns){
		this.table=table;
		this.columns=columns;
	}

	public Predicate where(String column){
		predicate=new Predicate(this,column);
		return predicate;
	}

	private boolean keyInColumns(String[] columns,String str){
		for(String column:columns){
			if(str.equals(column))return true;
		}
		return false;
	}

	private String[] getBackFillColumns(String[] queryColumns){
		List<String> result=new ArrayList<String>();
		String[] columnSet=null;
		if(columns==null || (columns.length==1 && columns[0].equals("*"))){
			// Scan all columns minus the one in the where clause
			columnSet=table.getColumns();
		}else{
			// Scan only a selected set of columns
			columnSet=columns;
		}
		for(String column:columnSet){
			if(!keyInColumns(queryColumns,column)){
				result.add(column);
			}
		}
		return result.toArray(new String[0]);
	}

	public ResultSet execute() throws IOException,JSONException{
		if(predicate==null){
			if(columns==null || (columns.length==1 && columns[0].equals("*"))){
				return table.scan();
			}else{
				return table.scan(columns);
			}
		}
		result=new ResultSet("query");
		result.startTimer();
		// First build object set using predicates
		ObjectSet set=executePredicate(predicate);
		// System.out.println("objectSet count "+set.getCount());
		// Now fill in missing values from columns using object set
		String[] backfill=getBackFillColumns(predicate.getColumnList());
		ResultSet rs=table.scan(set,backfill,null);
		result.addExecutionPlan(rs.getExecutionPlan());
		// result.setObjectList(rs.getObjectList());
		result.setObjectSet(rs.getObjectSet());
		result.endTimer();
		return result;
	}

	private ObjectSet executePredicate(Predicate predicate) throws IOException,JSONException{
		// ObjectSet set=new ObjectSet(false);
		if(predicate.getType()==Predicate.OR){

		}else if(predicate.getType()==Predicate.AND){

		}else if(predicate.getType()==Predicate.NOT){

		}else if(predicate.getType()==Predicate.WHERE){
			return executePredicate(predicate.getColumn(),predicate.getLeftChild());
		}else{

		}
		// return set;
		return null;
	}

	private ObjectSet executePredicate(String columnName,Predicate predicate) throws IOException,JSONException{
		// System.out.println("Executing predicate on "+columnName);
		// ObjectSet closedSet=new ObjectSet(false);
		ObjectSet openSet=new ObjectSet(true);
		ResultSet rs=table.scan(openSet,new String[]{columnName},predicate);
		long tStart=System.nanoTime();
		openSet.close();
		/*
		for(Variant v:openSet.getVariants(columnName)){
			// if(predicate.testVariant(v)){
				// System.out.println("Found one in "+columnName+" for "+v.getOID());
				closedSet.addOID(v.getOID());
				closedSet.addVariant(columnName,v);
			// }
		}*/
		long tEnd=System.nanoTime();
		result.addExecutionPlan(rs.getExecutionPlan());
		/*JSONObject obj=new JSONObject();
		obj.put("column",columnName);
		obj.put("operation","compare");
		obj.put("time",(tEnd-tStart));
		obj.put("variants.compared",openSet.getCount());
		result.addExecutionPlan(obj);*/
		return openSet;
	}
}