package com.pierre.spark.ml.feature_engineering;

import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class BasicFunction {
	
	
	public static JavaRDD<String> FilterString(JavaRDD<String> inputRDD, final String stringFilter, boolean contains ){
		
		JavaRDD<String> outputRDD; 
		
		if (contains == true){
			outputRDD =  inputRDD.filter(new Function <String, Boolean>(){
				public Boolean call(String s) throws Exception {
					Boolean result = s.contains(stringFilter);
					return result;
			}});
		}else{
			outputRDD =  inputRDD.filter(new Function <String, Boolean>(){
				public Boolean call(String s) throws Exception {
					Boolean result = !s.contains(stringFilter);
					return result;
				}});
		}
		
		return outputRDD; 
	}

	public static JavaRDD<String[]> MapToArrayWithSep(JavaRDD<String> inputRDD, final String sep){
		
		JavaRDD<String[]> outputRDD; 
		
		outputRDD = inputRDD.map(new Function <String, String[]>() {
							
					public String[] call(String line) throws Exception {
						
					return line.split(sep+"(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
					}});
		
		
		return outputRDD; 
	}

	public static JavaRDD<String> RemoveChar(JavaRDD<String> input, final String regex) {
		JavaRDD<String> output;
		output = input.map(new Function<String, String>() {
			public String call(String line) throws Exception {
				line = line.replaceAll(regex, "");
				return line;
			}
		});
		return output;
	}
	
	

}
