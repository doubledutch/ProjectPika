package me.doubledutch.pikadb;

import java.io.*;

public class LargeHash{
	private final static int SIZE=3; // Number of 64bit longs
	private long[] hash;

	public LargeHash(long[] data){
		this.hash=data;
	}

	public LargeHash(){
		this.hash=new long[SIZE];
	}

	public void addHash(LargeHash data){
		for(int i=0;i<SIZE;i++){
			hash[i]=hash[i]|data.hash[i];
		}
	}

	public boolean containsHash(LargeHash data){
		for(int i=0;i<SIZE;i++){
			if((hash[i] & data.hash[i]) != data.hash[i]){
				return false;
			}
		}
		return true;
		// (bloomfilter & bits) == bits;
	}

	public static int getSize(){ // actual size in bytes
		return SIZE*8;
	}

	public static int getIntegerCount(){
		return SIZE;
	}

	public static LargeHash read(DataInput in) throws IOException{
		long[] data=new long[SIZE];
		for(int i=0;i<SIZE;i++){
			data[i]=in.readLong();
		}
		return new LargeHash(data);
	}

	public void write(DataOutput out) throws IOException{
		for(int i=0;i<SIZE;i++){
			out.writeLong(hash[i]);
		}
	}

	public String toString(){
		StringBuilder buf=new StringBuilder();
		for(int i=0;i<SIZE;i++){
			if(i>0)buf.append("-");
			buf.append(Long.toBinaryString(hash[i]));
		}
		return buf.toString();
	}
}