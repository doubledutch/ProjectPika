package me.doubledutch.pikadb;

import java.io.*;

public abstract class Variant{
	public final static int STOP=0;
	public final static int INTEGER=1;
	public final static int FLOAT=2;
	public final static int STRING=3;

	public abstract int getOID();
	public abstract int getType();
	public abstract int getSize();
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
		if(obj instanceof java.lang.String){
			return new Variant.String(oid,(java.lang.String)obj);
		}
		return null;
	}

	public static Variant readVariant(DataInput in) throws IOException{
		byte type=in.readByte();
		if(type==STOP){
			return null;
		}
		int oid=in.readInt();
		switch(type){
			case INTEGER:return Variant.Integer.readValue(oid,in);
			case FLOAT:return Variant.Float.readValue(oid,in);
			case STRING:return Variant.String.readValue(oid,in);
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

		public int getSize(){
			return 1+4+4;
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

		public int getSize(){
			return 1+4+4;
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

	public static class String extends Variant{
		private int oid;
		private java.lang.String value;

		public String(int oid,java.lang.String value){
			this.oid=oid;
			this.value=value;
		}

		public int getOID(){
			return oid;
		}

		public int getSize(){
			return 1+4+2+2*value.length();
		}

		public int getType(){
			return STRING;
		}

		public Object getObjectValue(){
			return value;
		}

		public java.lang.String getValue(){
			return value;
		}

		public void writeVariant(DataOutput out) throws IOException{
			out.writeByte(STRING);
			out.writeInt(oid);
			out.writeShort((short)value.length());
			for(int i=0;i<value.length();i++){
				out.writeChar(value.charAt(i));
			}
		}

		public byte[] toByteArray() throws IOException{
			ByteArrayOutputStream data=new ByteArrayOutputStream();
			DataOutputStream out=new DataOutputStream(data);
			writeVariant(out);
			out.flush();
			out.close();
			return data.toByteArray();
		}

		public static Variant.String readValue(int oid,DataInput in) throws IOException{
			short length=in.readShort();
			char[] data=new char[length];
			for(int i=0;i<length;i++){
				data[i]=in.readChar();
			}
			return new Variant.String(oid,new java.lang.String(data));
		}
	}
}