package me.doubledutch.pikadb;

import java.io.*;
import org.json.*;
import java.util.*;

public abstract class Variant{
	public final static int STOP=0;
	public final static int INTEGER=1;
	public final static int FLOAT=2;
	public final static int STRING=3;
	public final static int DOUBLE=4;
	public final static int BOOLEAN=5;
	public final static int DELETE=6;
	public final static int SKIP=7;

	// public static Variant skipVariant=new Variant.Skip();

	public abstract int getOID();
	public abstract int getType();
	public abstract int getSize();
	public abstract Object getObjectValue();
	public abstract byte[] toByteArray() throws IOException;
	public abstract void writeVariant(DataOutput out) throws IOException;
	// public abstract void skipValue(DataInput in) throws IOException;

	public static void deleteValues(ObjectSet set,Page page) throws IOException{
		DataInput in=page.getDataInput();
		int offset=0;
		Variant v=readVariant(in,set);
		while(v!=null){
			// Read values
			if(set.contains(v.getOID())){
				byte[] deleteData=new byte[v.getSize()];
				for(int i=0;i<deleteData.length;i++){
					deleteData[i]=DELETE;
				}
				page.addDiff(offset,deleteData);
			}
			// ERROR: the offset is no longer correct with skip variants - fix
			offset+=v.getSize();
			v=readVariant(in,set);
		}
	}

	public static Variant createVariant(int oid,Object obj){
		if(obj instanceof java.lang.Integer){
			return new Variant.Integer(oid,(java.lang.Integer)obj);
		}
		if(obj instanceof java.lang.Float){
			return new Variant.Float(oid,(java.lang.Float)obj);
		}
		if(obj instanceof java.lang.Double){
			return new Variant.Double(oid,(java.lang.Double)obj);
		}
		if(obj instanceof java.lang.String){
			return new Variant.String(oid,(java.lang.String)obj);
		}
		if(obj instanceof java.lang.Boolean){
			return new Variant.Boolean(oid,(java.lang.Boolean)obj);
		}
		return null;
	}

	public static Variant readVariant(DataInput in,ObjectSet set) throws IOException{
		byte type=in.readByte();
		while(type==DELETE){
			type=in.readByte();
		}
		if(type==STOP){
			return null;
		}
		int oid=in.readInt();
		if(set.contains(oid)){
			switch(type){
				case INTEGER:return Variant.Integer.readValue(oid,in);
				case FLOAT:return Variant.Float.readValue(oid,in);
				case DOUBLE:return Variant.Double.readValue(oid,in);
				case STRING:return Variant.String.readValue(oid,in);
				case BOOLEAN:return Variant.Boolean.readValue(oid,in);
			}
		}else{
			switch(type){
				case INTEGER:return Variant.Integer.skipValue(in);
				case FLOAT:return Variant.Float.skipValue(in);
				case DOUBLE:return Variant.Double.skipValue(in);
				case STRING:return Variant.String.skipValue(in);
				case BOOLEAN:return Variant.Boolean.skipValue(in);
			}
			// return skipVariant;
		}
		return null;
	}

	public static class Skip extends Variant{
		private int size;

		public Skip(int size){
			this.size=size;
		}

		public int getSize(){
			return size;
		}

		public int getOID(){
			return -1;
		}

		public int getType(){
			return SKIP;
		}

		public Object getObjectValue(){
			return -1;
		}

		public int getValue(){
			return -1;
		}

		public void writeVariant(DataOutput out) throws IOException{

		}

		public byte[] toByteArray() throws IOException{
			return new byte[0];
		}

		public static Variant readValue(int oid,DataInput in) throws IOException{
			// return Variant.skipVariant;
			return null;
		}

		public static Variant skipValue(DataInput in) throws IOException{
			// return skipVariant;
			return null;
		}
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

		public static Variant skipValue(DataInput in) throws IOException{
			in.skipBytes(4);
			return new Variant.Skip(1+4+4);
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

	public static class Boolean extends Variant{
		private int oid;
		private boolean value;

		public Boolean(int oid,boolean value){
			this.oid=oid;
			this.value=value;
		}

		public int getSize(){
			return 1+4+1;
		}

		public int getOID(){
			return oid;
		}

		public int getType(){
			return BOOLEAN;
		}

		public Object getObjectValue(){
			return value;
		}

		public boolean getValue(){
			return value;
		}

		public void writeVariant(DataOutput out) throws IOException{
			out.writeByte(BOOLEAN);
			out.writeInt(oid);
			out.writeBoolean(value);
		}

		public byte[] toByteArray() throws IOException{
			ByteArrayOutputStream data=new ByteArrayOutputStream();
			DataOutputStream out=new DataOutputStream(data);
			writeVariant(out);
			out.flush();
			out.close();
			return data.toByteArray();
		}

		public static Variant skipValue(DataInput in) throws IOException{
			in.skipBytes(1);
			return new Variant.Skip(1+4+1);
		}

		public static Variant.Boolean readValue(int oid,DataInput in) throws IOException{
			return new Variant.Boolean(oid,in.readBoolean());
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

		public static Variant skipValue(DataInput in) throws IOException{
			in.skipBytes(4);
			return new Variant.Skip(1+4+4);
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

	public static class Double extends Variant{
		private int oid;
		private double value;

		public Double(int oid,double value){
			this.oid=oid;
			this.value=value;
		}

		public int getOID(){
			return oid;
		}

		public int getSize(){
			return 1+4+8;
		}

		public int getType(){
			return DOUBLE;
		}

		public Object getObjectValue(){
			return value;
		}

		public double getValue(){
			return value;
		}

		public static Variant skipValue(DataInput in) throws IOException{
			in.skipBytes(8);
			return new Variant.Skip(1+4+8);
		}

		public void writeVariant(DataOutput out) throws IOException{
			out.writeByte(DOUBLE);
			out.writeInt(oid);
			out.writeDouble(value);
		}

		public byte[] toByteArray() throws IOException{
			ByteArrayOutputStream data=new ByteArrayOutputStream();
			DataOutputStream out=new DataOutputStream(data);
			writeVariant(out);
			out.flush();
			out.close();
			return data.toByteArray();
		}

		public static Variant.Double readValue(int oid,DataInput in) throws IOException{
			return new Variant.Double(oid,in.readDouble());
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

		public static Variant skipValue(DataInput in) throws IOException{
			short s=in.readShort();
			in.skipBytes(s*2);
			return new Variant.Skip(1+4+2+2*s);
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