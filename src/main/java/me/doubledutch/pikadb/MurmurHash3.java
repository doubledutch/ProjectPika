/*
 * This simple and crude murmurhash3 implementation was derived from the
 * one provided by Austin Appleby.
 * 
 * MurmurHash3 was written by Austin Appleby, and is placed in the public
 * domain. The author hereby disclaims copyright to this source code.
 */

package me.doubledutch.pikadb;

import java.io.Serializable;
import java.nio.ByteBuffer;

final class MurmurHash3 {
  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  public static LargeHash getSelectiveBits(int oid){
    int h1=hashInt(0,oid);
    int h2=hashInt(h1,oid);
    // int h3=hashInt(h2,oid);
    // int h4=hashInt(h3,oid);

    int num=LargeHash.getIntegerCount();
    long[] data=new long[num];
    /* int m=Math.abs(h1)%64;
    long bits=1l<<m;
    data[Math.abs(h2)%num]=bits;

    m=Math.abs(h3)%64;
    bits=1l<<m;
    data[Math.abs(h4)%num]=bits;
    
    m=Math.abs(h3)%64;
    bits=1l<<m;
    data[Math.abs(h4)%num]=bits;
*/

    for(int i=0;i<num;i++){
      int m=Math.abs(h1+i*h2)%64;
      long bits=1l<<m;
      data[i]=bits;
    }
    return new LargeHash(data);
  }

  public static int hashInt(int seed,int input) {
    int k1 = mixK1(input);
    int h1 = mixH1(seed, k1);
    return fmix(h1, 4);
  }

  private static int mixK1(int k1) {
    k1 *= C1;
    k1 = Integer.rotateLeft(k1, 15);
    k1 *= C2;
    return k1;
  }

  private static int mixH1(int h1, int k1) {
    h1 ^= k1;
    h1 = Integer.rotateLeft(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  private static int fmix(int h1, int length) {
    h1 ^= length;
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;
    return h1;
  }
}