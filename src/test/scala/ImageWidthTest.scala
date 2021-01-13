package com.arushkharbanda.johnsnowlabs.image

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, FunSuite}

class ImageWidthTest extends FunSuite
{
  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()
  val sc=spark.sparkContext
  var rdd=sc.textFile("src/test/resources/images.txt").map(x=>x)
  rdd.foreach(println)
  val rdd2=rdd.map(x=>Row(x,Files.readAllBytes(Paths.get(x))))
  rdd2.foreach(println)
  val schema = new StructType()
    .add(StructField("path", StringType, true))
    .add(StructField("image", BinaryType, true))

  val df=spark.createDataFrame(rdd2,schema)
  df.show()
  val transformer = new ImageWidth()
    .setInputCol("image")
    .setOutputCol("count")

  val df2=transformer.transform(df)


  df2.show()

  test ("ImageWidth.transform_jpg" ) {

    //for jpg
    df2.where(col("path")==="src/test/resources/images/auto.jpg").show()
    assert((df2.where(col("path")==="src/test/resources/images/auto.jpg").collect()(0).get(2)==961))
  }

 test("ImageWidth.transform_png" )  {

  //for png
    df2.where(col("path")==="src/test/resources/images/blur.png").show()
  assert((df2.where(col("path")==="src/test/resources/images/blur.png").collect()(0).get(2)==500))

  }

}