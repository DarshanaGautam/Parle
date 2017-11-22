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
import scala.io.Source
//import sparkSession.implicits._



object testing {
  
  def main(args: Array[String]): Unit = {

		    	val conf = new SparkConf().setAppName("test").setMaster("local")
		    	val sc = new SparkContext(conf)
					val sparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
				//	val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc) 

				val tableList = args(0)
				
				for (line <- Source.fromFile(""+tableList+"").getLines) {
				  
				  // val namelist = line.split(',')
						println("The files are" + line)
							//	var sourceTbl = ""
								
               val df = spark.read.option("header", "true").
               option("inferSchema", "true").
               csv("hdfs://internal-hdp-master1:8020"+line)
               
				}//End For
  } // End main
  
}// End Object



               
               
            /*   df.saveAsTable("parle.base_extract")
								
								
							//	var antuitProdTablename = namelist(0)// Extracts the table name from the file
							//	var sourceDb = namelist(1) // Extracts the db name from the file
		
							//	val table = antuitProdTablename.substring(antuitProdTablename.indexOf(".")+1)
							
							//	val sourceTable = table.substring(5).toUpperCase()	
								

								try{
							//	hiveContext.sql("REFRESH TABLE "+antuitProdTablename)
								//	val timeStamp = Calendar.getInstance.getTime

											println("|||| ******************************************************************")
											//println("|||| TABLE NAME :: "+antuitProdTablename+" Start Time :: "+timeStamp+"")
																		
          /*val DRIVER = "com.mysql.jdbc.Driver" ;
		      val CONNECTION = "jdbc:mysql://lineage-mule-cluster.cluster-cpslmao02wkq.us-west-2.rds.amazonaws.com:3306" ;
		      val USER = "readonly" ;
		      val PASSWORD = "5u9pvWECL9DPtHUB" ;*/
											
											
											

									var sourceTableCount=0
											try{
												var connection:Connection = null
														Class.forName(DRIVER)
														connection = DriverManager.getConnection(CONNECTION, USER, PASSWORD)
														val statement = connection.createStatement()
														val resultSet = statement.executeQuery("select count(*) from "+sourceDb+"."+sourceTable+"")
														
														while ( resultSet.next() ) 
														{
															sourceTableCount = resultSet.getInt(1)
														}

									println("|||| source table "+sourceTable+" count :"+sourceTableCount)
									connection.close()

											}catch{
											case _ : Throwable => println("|||| Connectivity Failed for Table :"+sourceDb+"."+sourceTable+"")
											}
											
									if(sourceTableCount > 0)
									{

										if(table.startsWith("mule_")){
										  
										  println ("||| load data")
										  
											val sourceTable = table.substring(5).toUpperCase()
											
											val mulesoft = hiveContext.read.format("jdbc").
											option("url", "jdbc:mysql://lineage-mule-cluster.cluster-cpslmao02wkq.us-west-2.rds.amazonaws.com:3306").
											option("driver", "com.mysql.jdbc.Driver").
											option("dbtable", sourceDb+"."+sourceTable).
											option("user", "readonly").
											option("password", "5u9pvWECL9DPtHUB").load()  
											
											hiveContext.sql("drop table if exists anutittemp.incData"+table)
											hiveContext.sql("drop table if exists anutittemp.updatedData"+table)

									
											insrtDeltaIntoTable(mulesoft,antuitProdTablename,table,hiveContext) //passes 3 arguments :SourceData,antuitProdTablename,mule_sourcetablename
										}
									}
									else
									{
										println("|||| Source Table Empty : "+table)
									}

									
								}catch{
								case e : Throwable => (e.getStackTrace).foreach {  x => println( "||||"+x)} 
								}
				  }// End For Loop
				  }
	
}

  
  
  
  
  println("test")
}*/