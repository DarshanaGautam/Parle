package com.parle
import scala.io.Source
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
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
import java.io.File






object TableCreation {
  
   def main(args: Array[String]): Unit = {

		    	val conf = new SparkConf().setAppName("TableCreation").setMaster("local")
		    	val sc = new SparkContext(conf)
		    	val warehouseLocation = new File("spark-warehouse").getAbsolutePath
		    	val spark = SparkSession.builder()
		    	             .appName("Spark Hive Example")
		    	             .config("spark.sql.warehouse.dir", warehouseLocation)
                       .enableHiveSupport()
                       .getOrCreate()
		    	
         
				val tabledetails = args(0)
				val tablename = args(1)
				
				for (table <- Source.fromFile(tablename).getLines) {
				  
				  var sfatable=table
				  println ("Table name is " + sfatable)
				  var columnString =""	
				  val dropQuery = "drop table if exists antuit_prod.dotnet_bps_"+table.toLowerCase()
					var createQuery = "create external table antuit_prod.dotnet_bps_"+table.toLowerCase()+" ( "
					
				       for (line <- Source.fromFile(tabledetails).getLines)
				{
				  val namelist = line.split(',')
          var Tablename = namelist(0)// Extracts the table name from the file
					var columnName = namelist(1) // Extracts the column name from the file
					var dataType = namelist(2) // Extracts the data_type
						
					// println ("Table name is " + Tablename)
					
					  	if (sfatable==Tablename)
				   	{    
					  
							  columnString = columnString +columnName.toLowerCase()+"  ";  			
							  
											 if(dataType.toLowerCase().equals("varchar")){
												columnString+="string";
											}
											else if(dataType.toLowerCase().equals("bigint")){
													columnString+="bigint";
												}
											else if(dataType.toLowerCase().equals("numeric")){
													columnString+="double";
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
											else if(dataType.toLowerCase().equals("nvarchar")){
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
								}	
				  
					  	else { println (" Not equal :") }
								
			} 
					import spark.implicits._
          import spark.sql
          
					createQuery += columnString.substring(0,columnString.length()-1 ) + ") stored as orc LOCATION '/user/commonuser/'";
				  
					println(createQuery)
		
					sql(dropQuery)
					sql(createQuery)
				
			} 
			
          
   }// End main
   
}// End object
