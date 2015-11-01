package me.doubledutch.pikadb.query;

import me.doubledutch.pikadb.Variant;

public class Predicate{
	protected static final int EQUALS=0;
	protected static final int LESSTHAN=1;
	protected static final int GREATERTHAN=2;
	protected static final int LIKE=3;
	protected static final int ISNULL=4;
	protected static final int OR=5;
	protected static final int AND=6;
	protected static final int NOT=7;
	protected static final int WHERE=8;

	private Query query;
	private String column;
	private Variant value=null;
	private String pattern=null;
	private int type;
	private Predicate leftChild=null;
	private Predicate rightChild=null;

	protected Predicate(Query query,String column){
		this.column=column;
		this.type=WHERE;
		this.query=query;
	}

	protected Predicate(Query query,int type,Variant value){
		this.type=type;
		this.query=query;
		this.value=value;
	}

	protected Predicate(Query query,int type,String value){
		this.type=type;
		this.query=query;
		this.pattern=value;
	}

	protected int getType(){
		return type;
	}

	public Predicate equalTo(Variant v){
		Predicate p=new Predicate(query,EQUALS,v);
		leftChild=p;
		return p;
		// TODO: check that this can only be created on a where predicate
	}

	public Predicate lessThan(Variant v){
		Predicate p=new Predicate(query,LESSTHAN,v);
		leftChild=p;
		return p;
		// TODO: check that this can only be created on a where predicate
	}

	public Predicate greaterThan(Variant v){
		Predicate p=new Predicate(query,GREATERTHAN,v);
		leftChild=p;
		return p;
		// TODO: check that this can only be created on a where predicate
	}

	public Predicate like(String str){
		Predicate p=new Predicate(query,LIKE,str);
		leftChild=p;
		return p;
		// TODO: check that this can only be created on a where predicate
	}

/*
	public static Predicate isNull(){
		return new Predicate(ISNULL,null,null,null);
	}

	public Predicate or(Predicate p1){
		return new Predicate(OR,null,this,p1);
	}

	public Predicate and(Predicate p1){
		return new Predicate(AND,null,this,p1);
	}

	public Predicate not(){
		return new Predicate(NOT,null,this,null);
	}

	public boolean testVariant(Variant v){
		switch(type){
			case EQUALS:return v.equals(value);
			case OR:return leftChild.testVariant(v) || rightChild.testVariant(v);
		}
		return false;
	}*/
}