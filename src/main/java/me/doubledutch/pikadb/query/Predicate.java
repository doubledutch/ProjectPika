package me.doubledutch.pikadb.query;

import me.doubledutch.pikadb.Variant;

public class Predicate{
	private static final int EQUALS=0;
	private static final int LESSTHAN=1;
	private static final int GREATERTHAN=2;
	private static final int LIKE=3;
	private static final int ISNULL=4;
	private static final int OR=5;
	private static final int AND=6;
	private static final int NOT=7;

	private Variant value=null;
	private int type;
	private Predicate leftChild=null;
	private Predicate rightChild=null;

	public Predicate(int type,Variant value,Predicate left,Predicate right){
		this.type=type;
		this.value=value;
		this.leftChild=left;
		this.rightChild=right;
	}

	public static Predicate equalTo(Variant v){
		return new Predicate(EQUALS,v,null,null);
	}

	public static Predicate lessThan(Variant v){
		return new Predicate(LESSTHAN,v,null,null);
	}

	public static Predicate greaterThan(Variant v){
		return new Predicate(GREATERTHAN,v,null,null);
	}

	public static Predicate like(Variant v){
		return new Predicate(LIKE,v,null,null);
	}

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
	}
}