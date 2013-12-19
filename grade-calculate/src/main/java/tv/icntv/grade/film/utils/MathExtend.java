/* Copyright 2013 Future TV, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package tv.icntv.grade.film.utils;

import org.apache.hadoop.io.DoubleWritable;

import java.math.BigDecimal;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-26
 * Time: 下午2:29
 */
public class MathExtend {
    private static final int DEFAULT_DIV_SCALE = 10;
    /**
     * 提供（相对）精确的除法运算，当发生除不尽的情况时，精确到
     * 小数点以后10位，以后的数字四舍五入,舍入模式采用ROUND_HALF_EVEN
     * @param v1
     * @param v2
     * @return 两个参数的商
     */
    public static double divide(double v1, double v2)
    {
        return divide(v1, v2, DEFAULT_DIV_SCALE);
    }

    /**
     * 提供（相对）精确的除法运算。当发生除不尽的情况时，由scale参数指
     * 定精度，以后的数字四舍五入。舍入模式采用ROUND_HALF_EVEN
     * @param v1
     * @param v2
     * @param scale 表示需要精确到小数点以后几位。
     * @return 两个参数的商
     */
    public static double divide(double v1,double v2, int scale)
    {
        return divide(v1, v2, scale, BigDecimal.ROUND_HALF_EVEN);
    }

    /**
     * 提供（相对）精确的除法运算。当发生除不尽的情况时，由scale参数指
     * 定精度，以后的数字四舍五入。舍入模式采用用户指定舍入模式
     * @param v1
     * @param v2
     * @param scale 表示需要精确到小数点以后几位
     * @param round_mode 表示用户指定的舍入模式
     * @return 两个参数的商
     */
    public static double divide(double v1,double v2,int scale, int round_mode){
        if(scale < 0)
        {
            throw new IllegalArgumentException("The scale must be a positive integer or zero");
        }
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return divide(b1,b2,scale,round_mode);
    }

    public static double divide(BigDecimal v1,BigDecimal v2,int scale,int round_mode){
        return v1.divide(v2,scale,round_mode).doubleValue();
    }
//    public static double divide(long v1,long v2){
//        return divide(v1,v2,DEFAULT_DIV_SCALE);
//    }
//    public static double divide(long v1,long v2,int scale){
//        return divide(v1,v2,scale);
//    }
//    public static double divide(long v1,long v2,int scale,int round_mode){
//        if(scale < 0)
//        {
//            throw new IllegalArgumentException("The scale must be a positive integer or zero");
//        }
//        BigDecimal b1 = new BigDecimal(Double.toString(v1));
//        BigDecimal b2 = new BigDecimal(Double.toString(v2));
//        return divide(b1,b2,scale,round_mode);
//    }
    public static void main(String[]args){
        double d=divide(234L,34L,5);
        DoubleWritable dd= new DoubleWritable(d);
        System.out.println(dd.get());
    }
}
