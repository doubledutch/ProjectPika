package me.doubledutch.pikadb;

import java.io.*;

import me.doubledutch.pikadb.page.Page;

import org.json.*;

import java.util.*;

public abstract class Variant implements Comparable<Variant>{
	public final static int STOP=0;
	public final static int INTEGER=1;
	public final static int FLOAT=2;
	public final static int STRING=3;
	public final static int DOUBLE=4;
	public final static int BOOLEAN=5;
	public final static int DELETE=6;
	public final static int SKIP=7;
	public final static int LONG=8;

	// public static Variant skipVariant=new Variant.Skip();

	public abstract int getOID();
	public abstract int getType();
	public abstract int getSize();
	public abstract Object getObjectValue();
	public abstract byte[] toByteArray() throws IOException;
	public abstract void writeVariant(DataOutput out) throws IOException;
	// public abstract void skipValue(DataInput in) throws IOException;

	public abstract int compareTo(Variant v);

	public static void deleteValues(ObjectSet set,Page page) throws IOException{
		DataInput in=page.getDataInput();
		int offset=0;
		// TODO: we could be skipping our way through it here
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
			offset+=v.getSize();
			v=readVariant(in,set);
		}
	}

	public static Variant createVariant(int oid,Object obj){
		if(obj instanceof java.lang.Integer){
			return new Variant.Integer(oid,(java.lang.Integer)obj);
		}
		if(obj instanceof java.lang.String){
			return new Variant.String(oid,(java.lang.String)obj);
		}
		if(obj instanceof java.lang.Float){
			return new Variant.Float(oid,(java.lang.Float)obj);
		}
		if(obj instanceof java.lang.Double){
			return new Variant.Double(oid,(java.lang.Double)obj);
		}
		if(obj instanceof java.lang.Boolean){
			return new Variant.Boolean(oid,(java.lang.Boolean)obj);
		}
		if(obj instanceof java.lang.Long){
			return new Variant.Long(oid,(java.lang.Long)obj);
		}
		return null;
	}

	public static Variant readVariant(DataInput in,ObjectSet set) throws IOException{
		byte type=in.readByte();
		int count=0;
		if(type==DELETE){
			// TODO: check that this works and think about an accumulative fix
			return new Variant.Skip(1);
		}
		if(type==STOP){
			return null;
		}
		int oid=in.readInt();
		if(set.contains(oid)){
			switch(type){
				case INTEGER:return Variant.Integer.readValue(oid,in);
				case LONG:return Variant.Long.readValue(oid,in);
				case FLOAT:return Variant.Float.readValue(oid,in);
				case DOUBLE:return Variant.Double.readValue(oid,in);
				case STRING:return Variant.String.readValue(oid,in);
				case BOOLEAN:return Variant.Boolean.readValue(oid,in);
			}
		}else{
			switch(type){
				case INTEGER:return Variant.Integer.skipValue(in);
				case LONG:return Variant.Long.skipValue(in);
				case FLOAT:return Variant.Float.skipValue(in);
				case DOUBLE:return Variant.Double.skipValue(in);
				case STRING:return Variant.String.skipValue(in);
				case BOOLEAN:return Variant.Boolean.skipValue(in);
			}
		}
		return null;
	}

	public static class Skip extends Variant{
		private final int size;

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

		public int compareTo(Variant v){
			return 0;
		}
	}

	public static class Integer extends Variant{
		private final int oid;
		private final int value;

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

		// Returns a negative integer, zero, or a positive integer as this object is 
		// less than, equal to, or greater than the specified object.
		public int compareTo(Variant v){
			switch(v.getType()){
				case INTEGER:
					int ival=((Variant.Integer)v).getValue();
					if(ival<value)return -1;
					if(ival==value)return 0;
					return 1;
				case LONG:
					long lval=((Variant.Long)v).getValue();
					if(lval<value)return -1;
					if(lval==value)return 0;
					return 1;
				case FLOAT:
					float fval=((Variant.Float)v).getValue();
					if(fval<value)return -1;
					if(fval==value)return 0;
					return 1;
				case DOUBLE:
					double dval=((Variant.Double)v).getValue();
					if(dval<value)return -1;
					if(dval==value)return 0;
					return 1;
				case BOOLEAN:
					return -1;
				case STRING:
					java.lang.String sval=((Variant.String)v).getValue();
					java.lang.String str=java.lang.String.valueOf(value);
					return str.compareTo(sval);
			}
			return 0;
		}
	}

	public static class Boolean extends Variant{
		private final int oid;
		private final boolean value;

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

		public int compareTo(Variant v){
			switch(v.getType()){
				case INTEGER:
					return 0;
				case LONG:
					return 0;
				case FLOAT:
					return 0;
				case DOUBLE:
					return 0;
				case BOOLEAN:
					return 0;
				case STRING:
					return 0;
			}
			return 0;
		}
	}

	public static class Float extends Variant{
		private final int oid;
		private final float value;

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

		// Returns a negative integer, zero, or a positive integer as this object is 
		// less than, equal to, or greater than the specified object.
		public int compareTo(Variant v){
			switch(v.getType()){
				case INTEGER:
					int ival=((Variant.Integer)v).getValue();
					if(ival<value)return -1;
					if(ival==value)return 0;
					return 1;
				case LONG:
					long lval=((Variant.Long)v).getValue();
					if(lval<value)return -1;
					if(lval==value)return 0;
					return 1;
				case FLOAT:
					float fval=((Variant.Float)v).getValue();
					if(fval<value)return -1;
					if(fval==value)return 0;
					return 1;
				case DOUBLE:
					double dval=((Variant.Double)v).getValue();
					if(dval<value)return -1;
					if(dval==value)return 0;
					return 1;
				case BOOLEAN:
					return -1;
				case STRING:
					java.lang.String sval=((Variant.String)v).getValue();
					java.lang.String str=java.lang.String.valueOf(value);
					return str.compareTo(sval);
			}
			return 0;
		}
	}

	public static class Double extends Variant{
		private final int oid;
		private final double value;

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

		// Returns a negative integer, zero, or a positive integer as this object is 
		// less than, equal to, or greater than the specified object.
		public int compareTo(Variant v){
			switch(v.getType()){
				case INTEGER:
					int ival=((Variant.Integer)v).getValue();
					if(ival<value)return -1;
					if(ival==value)return 0;
					return 1;
				case LONG:
					long lval=((Variant.Long)v).getValue();
					if(lval<value)return -1;
					if(lval==value)return 0;
					return 1;
				case FLOAT:
					float fval=((Variant.Float)v).getValue();
					if(fval<value)return -1;
					if(fval==value)return 0;
					return 1;
				case DOUBLE:
					double dval=((Variant.Double)v).getValue();
					if(dval<value)return -1;
					if(dval==value)return 0;
					return 1;
				case BOOLEAN:
					return -1;
				case STRING:
					java.lang.String sval=((Variant.String)v).getValue();
					java.lang.String str=java.lang.String.valueOf(value);
					return str.compareTo(sval);
			}
			return 0;
		}
	}

	public static class Long extends Variant{
		private final int oid;
		private final long value;

		public Long(int oid,long value){
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
			return LONG;
		}

		public Object getObjectValue(){
			return value;
		}

		public long getValue(){
			return value;
		}

		public static Variant skipValue(DataInput in) throws IOException{
			in.skipBytes(8);
			return new Variant.Skip(1+4+8);
		}

		public void writeVariant(DataOutput out) throws IOException{
			out.writeByte(LONG);
			out.writeInt(oid);
			out.writeLong(value);
		}

		public byte[] toByteArray() throws IOException{
			ByteArrayOutputStream data=new ByteArrayOutputStream();
			DataOutputStream out=new DataOutputStream(data);
			writeVariant(out);
			out.flush();
			out.close();
			return data.toByteArray();
		}

		public static Variant.Long readValue(int oid,DataInput in) throws IOException{
			return new Variant.Long(oid,in.readLong());
		}

		// Returns a negative integer, zero, or a positive integer as this object is 
		// less than, equal to, or greater than the specified object.
		public int compareTo(Variant v){
			switch(v.getType()){
				case INTEGER:
					int ival=((Variant.Integer)v).getValue();
					if(ival<value)return -1;
					if(ival==value)return 0;
					return 1;
				case LONG:
					long lval=((Variant.Long)v).getValue();
					if(lval<value)return -1;
					if(lval==value)return 0;
					return 1;
				case FLOAT:
					float fval=((Variant.Float)v).getValue();
					if(fval<value)return -1;
					if(fval==value)return 0;
					return 1;
				case DOUBLE:
					double dval=((Variant.Double)v).getValue();
					if(dval<value)return -1;
					if(dval==value)return 0;
					return 1;
				case BOOLEAN:
					return -1;
				case STRING:
					java.lang.String sval=((Variant.String)v).getValue();
					java.lang.String str=java.lang.String.valueOf(value);
					return str.compareTo(sval);
			}
			return 0;
		}
	}

	public static class String extends Variant{
		private final int oid;
		private final java.lang.String value;

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

		// Returns a negative integer, zero, or a positive integer as this object is 
		// less than, equal to, or greater than the specified object.
		public int compareTo(Variant v){
			switch(v.getType()){
				case INTEGER:
					int ival=((Variant.Integer)v).getValue();
					java.lang.String sval=java.lang.String.valueOf(ival);
					return value.compareTo(sval);
				case LONG:
					long lval=((Variant.Long)v).getValue();
					sval=java.lang.String.valueOf(lval);
					return value.compareTo(sval);
				case FLOAT:
					float fval=((Variant.Float)v).getValue();
					sval=java.lang.String.valueOf(fval);
					return value.compareTo(sval);
				case DOUBLE:
					double dval=((Variant.Double)v).getValue();
					sval=java.lang.String.valueOf(dval);
					return value.compareTo(sval);
				case BOOLEAN:
					return -1;
				case STRING:
					sval=((Variant.String)v).getValue();
					return value.compareTo(sval);
			}
			return 0;
		}
	}
}