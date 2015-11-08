package me.doubledutch.pikadb;

import java.io.*;

public class LargeHash{
	private final static int SIZE=4; // Number of 32bit ints
	private int[] hash;

	public LargeHash(int[] data){
		this.hash=data;
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


}