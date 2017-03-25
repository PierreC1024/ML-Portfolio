package com.pierre.spark.ml.clustering.kmeans;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;

public class KMeansFindingK {

	public static int numberOfClusterWithVarianceGain(RDD<Vector> vector, Integer kmin, Integer kmax, Integer numInter,
			Double margin, String initializationMode, Integer initializationSteps, Double epsilon, Long seed) {

		KMeans kmeans = new KMeans().setInitializationMode(initializationMode)
									.setInitializationSteps(initializationSteps)
									.setEpsilon(epsilon)
									.setSeed(seed);
		KMeansModel Kmeans;
		Integer numberOfCluster = kmin;
		Double SumOfSquare = KMeansMetrics.SumOfSquare(vector);
		Double SumOfSquareNext;
		Boolean nextK = true;

		while (numberOfCluster <= kmax && nextK && SumOfSquare > 0) {
			Kmeans = kmeans.train(vector, numberOfCluster, numInter);
			SumOfSquareNext = Kmeans.computeCost(vector);
			nextK = SumOfSquareNext < margin * SumOfSquare;
			SumOfSquare = SumOfSquareNext;
			numberOfCluster += 1;
		}
		
		if (numberOfCluster > kmax){
			numberOfCluster = kmax ;
		}else{
			 numberOfCluster = numberOfCluster -2;
		}

		return numberOfCluster;
	}

}