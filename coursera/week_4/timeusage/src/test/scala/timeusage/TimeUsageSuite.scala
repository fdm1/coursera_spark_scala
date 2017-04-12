package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.functions.col
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

    test("'classifiedColumns' correctly sorts activity columns") {
      val columnsToSort = List("tfoo", "tbar",
                               "t01234", "t01439", "t0398", "t112321", "t180132", "t180313",
                               "t05237498", "t05132", "t1805123", "t18059",
                               "t9999","t1234")
      val (a,b,c) = classifiedColumns(columnsToSort)

      assert(a == List("t01234", "t01439", "t0398", "t112321", "t180132", "t180313").map(c => col(c)))
      assert(b == List("t05237498", "t05132", "t1805123", "t18059").map(c => col(c)))
      assert(c == List("t9999","t1234").map(c => col(c)))
    }

    test("'timeUsageSummary' aggregates all activites by age, gender, and employment") {
      val (columns, initDf) = read("/timeusage/atussum.csv")
      val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
      val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
      val expectedRows =
        Array(
          Row("working","female","elder", 2.0/30, 1.0/12, 7.0/60),
          Row("working","female","young", 2.0/15, 1.0/12, 15.0/100),
          Row("working","female","young", 1.0/10, 1.0/12, 13.0/60),
          Row("working","male","active", 1.0/60, 1.0/12, 1.0/4),
          Row("working","male","active", 1.0/12, 1.0/12, 1.0/5),
          Row("working","female","active", 2.0/30, 1.0/12, 13.0/60),
          Row("working","male","young", 1.0/5, 1.0/12, 11.0/60),
          Row("working","male","active", 7.0/60, 1.0/12, 13.0/60)
        )

      val summaryRows = summaryDf.collect

      assert(summaryRows.size == expectedRows.size)

      for (row <- expectedRows) { assert(summaryRows.contains(row)) }
    }
}
