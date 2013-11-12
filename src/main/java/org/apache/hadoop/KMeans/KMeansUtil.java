package org.apache.hadoop.KMeans;

import java.util.Vector;

/**
 * Created with IntelliJ IDEA.
 * User: YeonjuMac
 * Date: 13. 10. 19.
 * Time: 오후 7:59
 * To change this template use File | Settings | File Templates.
 */
public class KMeansUtil {

    public static Vector<Double> decodeVector(String[] tokens) {
        Vector<Double> v = new Vector<Double>();
        for (int i = 0; i < tokens.length; i++) {
            v.add(Double.parseDouble(tokens[i]));
        }
        return v;
    }

    public static boolean isNumeric(String token) {
        return token.matches("[-+]?\\d*\\.?\\d+");
    }

}
