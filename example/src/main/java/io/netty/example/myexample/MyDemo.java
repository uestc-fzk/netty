package io.netty.example.myexample;

import io.netty.handler.traffic.GlobalTrafficShapingHandler;

/**
 * @author fzk
 * @datetime 2022-10-26 11:38
 */
public class MyDemo {
    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException {
        long val = 8796093022208L;
        long bitmapIdx = val & 0xffff;
        long isSubPage =val&0x10000;
        long isUsed = val&0x20000;
        long pageSize =(val>>0x2ffff)&0xef;
        long runOffset = val>>0x1ffffff;
        System.out.printf("runOffset=%d pageSize=%d isUsed=%d isSubPage=%d bitmapIdx=%d\n",runOffset,pageSize,isUsed,isSubPage,bitmapIdx);

        GlobalTrafficShapingHandler handler = new GlobalTrafficShapingHandler(null, 1L, 1L, 1000L, 1000L);
    }
}
