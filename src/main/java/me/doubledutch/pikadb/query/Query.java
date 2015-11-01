package me.doubledutch.pikadb.query;

import java.io.*;
import org.json.*;
import me.doubledutch.pikadb.*;

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
		return new Predicate(this,column);
	}

	public ResultSet execute() throws IOException,JSONException{
		if(predicate==null){
			if(columns==null || (columns.length==1 && columns[0].equals("*"))){
				return table.scan();
			}else{
				return table.scan(columns);
			}
		}
		result=new ResultSet();
		result.startTimer();
		// First build object set using predicates
		ObjectSet set=executePredicate(predicate);
		// Now fill in missing values from columns using object set
		if(columns==null || (columns.length==1 && columns[0].equals("*"))){
			// Scan all columns minus the one in the where clause
		}else{
			// Scan only a selected set of columns
		}
		result.endTimer();
		return result;
	}

	private ObjectSet executePredicate(Predicate predicate) throws IOException,JSONException{
		ObjectSet set=new ObjectSet(false);
		if(predicate.getType()==Predicate.OR){

		}else if(predicate.getType()==Predicate.AND){

		}else if(predicate.getType()==Predicate.NOT){

		}else if(predicate.getType()==Predicate.WHERE){
			return executePredicate(predicate.getColumn(),predicate.getLeftChild());
		}else{

		}
		return set;
	}

	private ObjectSet executePredicate(String columnName,Predicate predicate) throws IOException,JSONException{
		ObjectSet closedSet=new ObjectSet(false);
		ObjectSet openSet=new ObjectSet(true);
		ResultSet rs=table.scan(openSet,new String[]{columnName});
		for(Variant v:openSet.getVariants(columnName)){
			if(predicate.testVariant(v)){
				closedSet.addVariant(columnName,v);
			}
		}
		return closedSet;
	}
}