package org.apache.hadoop.KMeans.DistanceMeasure;

import java.util.Vector;

/**
 * Created with IntelliJ IDEA.
 * User: YeonjuMac
 * Date: 13. 10. 19.
 * Time: 오후 7:25
 * To change this template use File | Settings | File Templates.
 */
public interface DistanceMeasure {
    public double getDistanced(double[] point, double[] centroid);
    public double getDistancev(Vector<Double> p1, Vector<Double> p2);
}
