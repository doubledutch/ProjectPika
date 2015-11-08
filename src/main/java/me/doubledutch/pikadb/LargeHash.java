package me.doubledutch.pikadb;

import java.io.*;

public class LargeHash{
	private final static int SIZE=8; // Number of 32bit ints
	private int[] hash;

	public LargeHash(int[] data){
		this.hash=data;
	}

	public LargeHash(){
		this.hash=new int[SIZE];
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
		return SIZE*4;
	}

	public static int getIntegerCount(){
		return SIZE;
	}

	public static LargeHash read(DataInput in) throws IOException{
		int[] data=new int[SIZE];
		for(int i=0;i<SIZE;i++){
			data[i]=in.readInt();
		}
		return new LargeHash(data);
	}

	public void write(DataOutput out) throws IOException{
		for(int i=0;i<SIZE;i++){
			out.writeInt(hash[i]);
		}
	}

	public String toString(){
		StringBuilder buf=new StringBuilder();
		for(int i=0;i<SIZE;i++){
			if(i>0)buf.append("-");
			buf.append(Integer.toBinaryString(hash[i]));
		}
		return buf.toString();
	}
}