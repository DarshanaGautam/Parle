package com.parle
import scala.io.Source
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import java.security.MessageDigest
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._
import scala.io.Source


object TableCreation {
  
   def main(args: Array[String]): Unit = {

		    	val conf = new SparkConf().setAppName("TableCreation").setMaster("local")
		    	val sc = new SparkContext(conf)
					val sqlContext= new org.apache.spark.sql.SQLContext(sc)
		    	val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("TableCreation").getOrCreate;
          import sqlContext.implicits._
			
				val tabledetails = args(0)
				val tablename = arges(1)
				
				for (table <- Source.fromFile(tablename).getLines) {
				  
				  var sfatable=table
				  
				  val dropQuery = "drop table if exists default.sfa_"+table.toLowerCase()
					var createQuery = "create external table default.sfa_"+table.toLowerCase()+" ( "
				  
				
				
				for (line <- Source.fromFile(tabledetails).getLines) {
				  
				  val namelist = line.split(',')
          var Tablename = namelist(0)// Extracts the table name from the file
					var columnName = namelist(1) // Extracts the column name from the file
					var dataType = namelist(2) // Extracts the data_type
					
					//val table=nameslist(0)
  
              val statement = connection.createStatement()
							//val resultSet = statement.executeQuery("select" +columnName+","+datatype+"From"+line+ "where TABLE_NAME = \'"+tbl+"\' and TABLE_SCHEMA=\'"+db+"\'")
							//val dropQuery = "drop table if exists "+dbName+".mule_"+tbl.toLowerCase()
							//var createQuery = "create external table "+dbName+".mule_"+tbl.toLowerCase()+" ( "
							
							var columnString =""

							while ( sfatable=Tablename ) {
							    	//val columnName = resultSet.getString(1)
										//val dataType = resultSet.getString(2)
										
											 if(dataType.toLowerCase().equals("varchar")){
												columnString+="string";
											}
											else if(dataType.toLowerCase().equals("bigint")){
													columnString+="bigint";
												}
											else if(dataType.toLowerCase().equals("longtext")){
													columnString+="string";
												}
											else if(dataType.toLowerCase().equals("datetime")){
													columnString+="string";
  											}
											else if(dataType.toLowerCase().equals("int")){
											  	columnString+="int";
												}
											else if(dataType.toLowerCase().equals("tinyint")){
													columnString+="int";
												}
											else 	if(dataType.toLowerCase().equals("decimal")){
												 	columnString+="double";
												}
											else if(dataType.toLowerCase().equals("double")){
													columnString+="double";
												}
											else if(dataType.toLowerCase().equals("tinyblob")){
													//System.out.println("....................................."+dataType.toLowerCase());
													columnString+="string";
												}
											else if(dataType.toLowerCase().equals("char")){
													columnString+="string";
										  	}
											else if(dataType.toLowerCase().equals("date")){
													columnString+="string";
												}
											else if(dataType.toLowerCase().equals("timestamp")){
													columnString+="string";
											}
											else if(dataType.toLowerCase().equals("bit")){
													columnString+="string";
											}		
											else if(dataType.toLowerCase().equals("blob")){
													columnString+="string";
											}
											else if(dataType.toLowerCase().equals("float")){
													columnString+="double";
											}
											else if(dataType.toLowerCase().equals("text")){
													columnString+="string";
											}
											else if(dataType.toLowerCase().equals("mediumblob")){
													columnString+="string";
											}
											else if(dataType.toLowerCase().equals("longblob")){
													columnString+="string";
											}
											else if(dataType.toLowerCase().equals("set")){
													columnString+="string";
											}
											else if(dataType.toLowerCase().equals("enum")){
													columnString+="string";
											}
											else if(dataType.toLowerCase().equals("mediumtext")){
													columnString+="string";
											}
											else if(dataType.toLowerCase().equals("smallint")){
													columnString+="int ";
											}
											else if(dataType.toLowerCase().equals("time")){
													columnString+="string";
											}
											 columnString +=",";
										}	// End while	
								
				} // End inner for 
					//createQuery += columnString.substring(0,columnString.length()-1 ) + ",fac_id string ,sequence bigint, ingestedtime timestamp ) PARTITIONED BY (ingestion_year int, ingestion_month int)  stored as parquet";
					 //createQuery += columnString.substring(0,columnString.length()-1 ) + ", sequence bigint , ingestedtime timestamp)  PARTITIONED BY (ingestion_year int, ingestion_month int) stored as parquet";
					createQuery += columnString.substring(0,columnString.length()-1 ) + ") stored as parquet";
				  println(createQuery)
				//	print(dropQuery)
					hiveContext.sql(dropQuery)
					hiveContext.sql(createQuery)
				} // End outer For
					
   }// End main
   
}// End object
