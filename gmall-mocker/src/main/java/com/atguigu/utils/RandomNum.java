package com.atguigu.utils;

/**
 * @author Skipper
 * @date 2020/08/14
 * @desc
 */
import java.util.Random;

public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}

