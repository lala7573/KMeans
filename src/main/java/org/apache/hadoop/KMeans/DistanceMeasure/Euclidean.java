package org.apache.hadoop.KMeans.DistanceMeasure;

import java.util.Vector;

/**
 * Created with IntelliJ IDEA.
 * User: YeonjuMac
 * Date: 13. 10. 21.
 * Time: 오후 4:01
 * To change this template use File | Settings | File Templates.
 */
public class Euclidean implements DistanceMeasure {
    double a=0, b=0;
    double result = 0.0;
    @Override
    public double getDistanced(double[] p1, double[] p2) {
        result = 0.0;
        for(int i = 0 ; i < p1.length ; i++){
            result += Math.pow( p1[i] - p2[i], 2);
        }
        return Math.sqrt(result);  //To change body of implemented methods use File | Settings | File Templates.
    }
    public double getDistancev(Vector<Double> p1, Vector<Double> p2){
        result = 0.0;
        for(int i = 0 ; i < p1.size(); i++){
            if(p1.get(i) instanceof Number && p2.get(i) instanceof  Number)
                result += Math.pow((p1.get(i) - p2.get(i)), 2);
        }
        return Math.sqrt(result);
    }
}
