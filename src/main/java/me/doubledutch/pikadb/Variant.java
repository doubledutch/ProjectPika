package me.doubledutch.pikadb;

import java.io.*;

public abstract class Variant{
	public final static int INTEGER=1;
	public final static int FLOAT=2;

	public abstract int getOID();
	public abstract int getType();
	public abstract Object getObjectValue();
	public abstract byte[] toByteArray() throws IOException;
	public abstract void writeVariant(DataOutput out) throws IOException;

	public static Variant createVariant(int oid,Object obj){
		if(obj instanceof java.lang.Integer){
			return new Variant.Integer(oid,(java.lang.Integer)obj);
		}
		if(obj instanceof java.lang.Float){
			return new Variant.Float(oid,(java.lang.Float)obj);
		}
		return null;
	}

	public static Variant readVariant(DataInput in) throws IOException{
		byte type=in.readByte();
		int oid=in.readInt();
		switch(type){
			case INTEGER:return Variant.Integer.readValue(oid,in);
			case FLOAT:return Variant.Float.readValue(oid,in);
		}
		return null;
	}

	public static class Integer extends Variant{
		private int oid;
		private int value;

		public Integer(int oid,int value){
			this.oid=oid;
			this.value=value;
		}

		public int getOID(){
			return oid;
		}

		public int getType(){
			return INTEGER;
		}

		public Object getObjectValue(){
			return value;
		}

		public int getValue(){
			return value;
		}

		public void writeVariant(DataOutput out) throws IOException{
			out.writeByte(INTEGER);
			out.writeInt(oid);
			out.writeInt(value);
		}

		public byte[] toByteArray() throws IOException{
			ByteArrayOutputStream data=new ByteArrayOutputStream();
			DataOutputStream out=new DataOutputStream(data);
			writeVariant(out);
			out.flush();
			out.close();
			return data.toByteArray();
		}

		public static Variant.Integer readValue(int oid,DataInput in) throws IOException{
			return new Variant.Integer(oid,in.readInt());
		}
	}

	public static class Float extends Variant{
		private int oid;
		private float value;

		public Float(int oid,float value){
			this.oid=oid;
			this.value=value;
		}

		public int getOID(){
			return oid;
		}

		public int getType(){
			return FLOAT;
		}

		public Object getObjectValue(){
			return value;
		}

		public float getValue(){
			return value;
		}

		public void writeVariant(DataOutput out) throws IOException{
			out.writeByte(FLOAT);
			out.writeInt(oid);
			out.writeFloat(value);
		}

		public byte[] toByteArray() throws IOException{
			ByteArrayOutputStream data=new ByteArrayOutputStream();
			DataOutputStream out=new DataOutputStream(data);
			writeVariant(out);
			out.flush();
			out.close();
			return data.toByteArray();
		}

		public static Variant.Float readValue(int oid,DataInput in) throws IOException{
			return new Variant.Float(oid,in.readFloat());
		}
	}
}