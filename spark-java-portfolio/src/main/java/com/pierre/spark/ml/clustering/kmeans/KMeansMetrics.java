package com.pierre.spark.ml.clustering.kmeans;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.rdd.RDD;

public class KMeansMetrics {
	
	public static Double SumOfSquare(RDD<Vector> vector){
		Vector varianceVector;
		Integer vectorSize; 
		Double sumOfSquare = 0.; 
		Long numberOfElements; 
		
		numberOfElements = vector.count();
		varianceVector = Statistics.colStats(vector).variance();
		vectorSize = varianceVector.size() - 1;
		while (vectorSize >= 0) {
			sumOfSquare += varianceVector.apply(vectorSize);
			vectorSize -= 1;
		}
		sumOfSquare = (numberOfElements - 1) * sumOfSquare;
		return sumOfSquare;
	}
}
