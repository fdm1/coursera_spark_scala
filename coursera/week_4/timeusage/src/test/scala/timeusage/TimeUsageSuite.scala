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
      assert(columns == List("tucaseid","teage","telfs","tesex","tfoo","t01","t02","t03","t18","t05","t07"))
//    assert contents of initDf.first
//    assert schema of initDf
    }

    test("'row' keeps first value as String and all other values to Double") {
//    assert some stuff
    }

}
