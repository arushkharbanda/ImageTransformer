package com.arushkharbanda.johnsnowlabs.image

import java.io.ByteArrayInputStream

import javax.imageio.ImageIO
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class ImageWidth(override val uid: String)
  extends Transformer {

  def this() = this(Identifiable.randomUID("ImageWidth"))

  def copy(extra: ParamMap): ImageWidth = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {

    val idx = schema.fieldIndex(getInputCol)
    val field = schema.fields(idx)
    if (field.dataType != BinaryType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type BinaryType")
    }

    schema.add(StructField(getOutputCol, IntegerType, false))
  }

  override def transform(df: Dataset[_]): DataFrame = {
    val getWidth = udf { in: Array[Byte] =>

      val is = new ByteArrayInputStream(in)
      ImageIO.read(is).getWidth()
    }
    df.select(col("*"),
      getWidth(df.col(getInputCol)).as(getOutputCol))
  }

  val inputCol: Param[String] = new Param[String](this, "inputCol", "input image Array column")
  val outputCol: Param[String] = new Param[String](this, "outputCol", "output Int column output")
  def setInputCol(value: String): this.type = set(inputCol, value)
  def getInputCol: String = $(inputCol)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def getOutputCol: String = $(outputCol)
}



