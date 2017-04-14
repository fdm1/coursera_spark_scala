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
                    , 4.asInstanceOf[Double]
                    , 1.asInstanceOf[Double]
                    , 0.asInstanceOf[Double]
                    , 0.asInstanceOf[Double]
                    , 67.asInstanceOf[Double]
                    , 302.asInstanceOf[Double]
                    , 60.asInstanceOf[Double]
                    , 239.asInstanceOf[Double]
                    , 65.asInstanceOf[Double])
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
                               "t9999","t1234", "t0138", "t0145")
      val (a,b,c) = classifiedColumns(columnsToSort)

      assert(a == List("t01234", "t01439", "t0398", "t112321", "t180132", "t180313", "t0138", "t0145").map(c => col(c)))
      assert(b == List("t05237498", "t05132", "t1805123", "t18059").map(c => col(c)))
      assert(c == List("t9999","t1234").map(c => col(c)))
    }
    
    def getSummary(): DataFrame = {
      val (columns, initDf) = read("/timeusage/atussum.csv")
      val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
      val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
      summaryDf
    }

    test("'timeUsageSummary' aggregates all activites by age, gender, and employment") {
      val summaryDf = getSummary
      val expectedData =
        Array(
          Row("not working",   "male",       "elder",   302.0/60,   239.0/60, 192.0/60),
          Row("not working",   "male",       "elder",   239.0/60,   370.0/60, 1045.0/60),
          Row("working",       "female",     "young",   299.0/60,   121.0/60, 373.0/60),
          Row("working",       "female",     "young",   418.0/60,   367.0/60, 621.0/60),
          Row("working",       "male",       "elder",   190.0/60,   358.0/60, 133.0/60),
          Row("not working",   "male",       "active",  373.0/60,   129.0/60, 550.0/60),
          Row("not working",   "female",     "young",   422.0/60,   359.0/60, 616.0/60),
          Row("working",       "male",       "active",  554.0/60,   363.0/60, 623.0/60),
          Row("working",       "male",       "active",  548.0/60,   186.0/60, 361.0/60),
          Row("working",       "female",     "active",  786.0/60,   422.0/60, 1209.0/60),
          Row("working",       "female",     "elder",   477.0/60,   131.0/60, 247.0/60),
          Row("not working",   "male",       "active",  299.0/60,   177.0/60, 382.0/60),
          Row("working",       "male",       "active",  305.0/60,   357.0/60, 796.0/60),
          Row("not working",   "female",     "elder",   436.0/60,   176.0/60, 376.0/60),
          Row("working",       "female",     "young",   785.0/60,   190.0/60, 1028.0/60),
          Row("working",       "female",     "active",  184.0/60,   70.0/60,  864.0/60),
          Row("not working",   "female",     "active",  484.0/60,   60.0/60,  549.0/60),
          Row("not working",   "female",     "active",  785.0/60,   68.0/60,  247.0/60),
          Row("not working",   "female",     "young",   551.0/60,   423.0/60, 848.0/60),
          Row("not working",   "female",     "young",   368.0/60,   0.0/60,   784.0/60),
          Row("working",       "female",     "elder",   479.0/60,   179.0/60, 916.0/60),
          Row("working",       "male",       "active",  727.0/60,   180.0/60, 610.0/60)
        )

      val summaryData = summaryDf.collect

      assert(summaryData.size == expectedData.size)

      for (row <- summaryData) { assert(expectedData.contains(row)) }
    }

    test("'timeUsageGrouped' aggregates correct average per working status, sex, and age") {
      val summaryDf = getSummary
      val sparkDf = timeUsageGrouped(summaryDf)

      val expectedData =
        Array(
          Row("not working",  "female", "active", 10.6, 1.1,  6.6),
          Row("not working",  "female", "elder",  7.3,  2.9,  6.3),
          Row("not working",  "female", "young",  7.5,  4.3,  12.5),
          Row("not working",  "male",   "active", 5.6,  2.6,  7.8),
          Row("not working",  "male",   "elder",  4.5,  5.1,  10.3),
          Row("working",      "female", "active", 8.1,  4.1,  17.3),
          Row("working",      "female", "elder",  8.0,  2.6,  9.7),
          Row("working",      "female", "young",  8.3,  3.8,  11.2),
          Row("working",      "male",   "active", 8.9,  4.5,  10.0),
          Row("working",      "male",   "elder",  3.2,  6.0,  2.2)
        ) 

      val dfData = sparkDf.collect

      assert(dfData.size == expectedData.size)

      for (row <- dfData) { assert(expectedData.contains(row)) }
    }

    test("'timeUsageGrouped', 'timeUsageGroupedSql', and 'timeUsageSummaryTyped' all output the same results") {
      val summaryDf = getSummary
      val sparkDf = timeUsageGrouped(summaryDf)
      val sqlDf = timeUsageGroupedSql(summaryDf)
      val typedDf = timeUsageGroupedTyped(timeUsageSummaryTyped(summaryDf))

      val dfData = sparkDf.collect
      val sqlData = sqlDf.collect
      val typedData = typedDf.collect
      val typedUntypedData = typedData.map(r => Row(r.working, r.sex, r.age, r.primaryNeeds, r.work, r.other))

      assert(dfData.size == sqlData.size)
      assert(dfData.size == typedData.size)
      for (i <- dfData) {
        assert(sqlData.contains(i))
        assert(typedUntypedData.contains(i))
      }
    }
}
