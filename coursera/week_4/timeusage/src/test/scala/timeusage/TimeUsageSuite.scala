package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

import TimeUsage._

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
    test("'read' can read raw data") {
      val (columns, initDf) = read("/timeusage/atussum.csv")
      val firstRow = initDf.first
      val expectedFirstRowList =
        List("\"1\"", 60.asInstanceOf[Double]
                    , 2.asInstanceOf[Double]
                    , 2.asInstanceOf[Double]
                    , 0.asInstanceOf[Double]
                    , 1.asInstanceOf[Double]
                    , 1.asInstanceOf[Double]
                    , 3.asInstanceOf[Double]
                    , 0.asInstanceOf[Double]
                    , 5.asInstanceOf[Double]
                    , 6.asInstanceOf[Double])
      assert(firstRow ==  Row.fromSeq(expectedFirstRowList))
    }

    test("'dfSchema' generates correct schema") {
      val (columns, initDf) = read("/timeusage/atussum.csv")
      assert(columns == List("tucaseid","teage","telfs","tesex","tfoo","t01","t02","t03","t18","t05","t07"))

      val schema = StructType(Array(StructField("tucaseid",StringType,false),
                                    StructField("teage",DoubleType,false),
                                    StructField("telfs",DoubleType,false),
                                    StructField("tesex",DoubleType,false),
                                    StructField("tfoo",DoubleType,false),
                                    StructField("t01",DoubleType,false),
                                    StructField("t02",DoubleType,false),
                                    StructField("t03",DoubleType,false),
                                    StructField("t18",DoubleType,false),
                                    StructField("t05",DoubleType,false),
                                    StructField("t07",DoubleType,false)))

      assert(dfSchema(columns) == schema)
    }

    test("'row' keeps first value as String and all other values to Double") {
      val vals = List("1","2","3","4")
      val res = Row.fromSeq(List("1", 2.asInstanceOf[Double]
                                    , 3.asInstanceOf[Double]
                                    , 4.asInstanceOf[Double]))
      assert(row(vals) == res)
    }

}
