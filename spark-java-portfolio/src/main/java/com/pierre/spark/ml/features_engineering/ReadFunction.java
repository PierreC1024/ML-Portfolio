package com.pierre.spark.ml.features_engineering;

import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ReadFunction {
	
	public static JavaRDD<Row> ReadToJavaRDDRow (String path, StructType dataSchema, String sep, 
			JavaSparkContext sparkContext, boolean test) throws Exception{
		
		JavaRDD<String> inputRDD;
		JavaRDD<String[]> interJavaRDD; 
		JavaRDD<Row> outputRDD; 
		StructField[] fields = dataSchema.fields();
		
		inputRDD = sparkContext.textFile(path);
		inputRDD = BasicFunction.FilterString(inputRDD, fields[0].name(), false);
		interJavaRDD = BasicFunction.MapToArrayWithSep(inputRDD, sep);
		
		if (test == true){
		if(fields.length != interJavaRDD.first().length){
			System.out.println("The number of columns in the input dataset not matched with the input schema ! "
					+ "(" + fields.length + ", " + interJavaRDD.first().length + ")");
			System.out.println(Arrays.toString(interJavaRDD.first()));
			return null;
		}else{
			System.out.println("The number of columns in the input dataset matched with the input schema ! "
					+ "(" + fields.length + ", " + interJavaRDD.first().length + ")");
		}
		}
		outputRDD = MapToRowRelativeFields(interJavaRDD, fields);
		
		return outputRDD;
			
	}
	
	public static JavaRDD<Row> MapToRowRelativeFields(JavaRDD<String[]> inputRDD, final StructField[] fields){
		
		JavaRDD<Row> outputRDD; 
		final Integer lengthField = fields.length;
		final Object[] FeatureColumns = new Object[lengthField]; 
		
		outputRDD = inputRDD.map(new Function <String[], Row>(){
			public Row call(String[] list) throws Exception{
			
				for (int i=0; i < lengthField; i++){
					
					if(fields[i].dataType().toString()=="TimestampType"){
						FeatureColumns[i] = Timestamp.valueOf(list[i]);
						
					}else if(fields[i].dataType().toString()=="StringType"){
						FeatureColumns[i] = String.valueOf(list[i]);
						
					}else if(fields[i].dataType().toString()=="IntegerType"){
						FeatureColumns[i] = Integer.valueOf(list[i]);
					}
					else if(fields[i].dataType().toString()=="LongType"){
						FeatureColumns[i] = Long.valueOf(list[i]);
					}
					else if(fields[i].dataType().toString()=="DoubleType"){
						FeatureColumns[i] = Double.valueOf(list[i]);
					}
				}
				Row retRow = RowFactory.create(FeatureColumns);											
				return retRow;
			}
		});
		
		return outputRDD; 
		
	}
}
