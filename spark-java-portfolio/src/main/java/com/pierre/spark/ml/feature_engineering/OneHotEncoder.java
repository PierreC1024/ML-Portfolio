package com.pierre.spark.ml.feature_engineering;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class OneHotEncoder {
	
	public static Dataset<Row> OneHotEncoderDataset(Dataset<Row> dataset, String feature,
				SparkSession sparkSession){
			
			final List<String> strategyDropDuplicates;
			Dataset<Row> datasetFinal;
			final Integer size;
			StructType structType = DataTypes.createStructType(new StructField[0]);
			
			strategyDropDuplicates = FeatureExtractDropDublicates(dataset, feature);
		
			
			for (String valueName : strategyDropDuplicates)  {
				structType = structType.add(DataTypes.createStructField(feature + "_"
							+ valueName, DataTypes.IntegerType, false));
				}
	
			structType = structType.add(DataTypes.createStructField("index", DataTypes.LongType, false));
			
			size = strategyDropDuplicates.size();
			JavaRDD<Row> strategyRow = dataset.select(feature)
					.javaRDD()
					.zipWithIndex()
					.map(new Function<Tuple2<Row, Long>, Row>(){
						public Row call(Tuple2<Row, Long> tuple) throws Exception{
							Object[] FeatureColumns = new Object[size+1];
							Integer Feature=0;
							for (Integer i = 0 ; i < size ; i++){
								Feature = String.valueOf(tuple._1.get(0)).contains(strategyDropDuplicates.get(i)) ?
										1 : 0 ;
								FeatureColumns[i] = Feature;
							}
							
							FeatureColumns[size] = tuple._2.longValue(); 
							Row retRow = RowFactory.create(FeatureColumns);											
							return retRow;
						}
					});
			datasetFinal = sparkSession.createDataFrame(strategyRow, structType);
			
			return datasetFinal;
	}
	
	private static List<String> FeatureExtractDropDublicates(Dataset<Row> dataset, String feature){
		
		List<String> featureDropDuplicate;
		
		featureDropDuplicate = dataset.select(feature)
				 .javaRDD()
				 .distinct()
				 .map(new Function<Row, String>() {
						public String call(Row iRow) throws Exception {
							return String.valueOf(iRow.getInt(0));
						}
					})
				 .collect();
		return featureDropDuplicate;
	}
	
	

}
