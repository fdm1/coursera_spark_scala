package stackoverflow

import StackOverflow._
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.net.URL
import java.nio.channels.Channels
import java.io.File
import java.io.FileOutputStream

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }


  def samplePostings(): List[String] = {
    List("1,27233496,,,0,C#",
         "1,5484340,,,0,C#",
         "2,5494879,,5484340,1,",
         "1,9419744,,,2,Objective-C",
         "1,9002524,,,2,,",
         "2,9003401,,9002524,4,",
         "1,9002525,,,2,C++",
         "2,9003401,,9002525,4,",
         "2,9005311,,9002525,0,",
         "1,5257894,,,1,Java",
         "1,21984912,,,0,Java",
         "2,21985273,,21984912,0,")
  }


  test("'rawPostings' should convert strings into 'Postings'") {
    val lines = sc.parallelize(samplePostings)
    val raw = rawPostings(lines)

    val expectedPosting = Posting(1,27233496,None,None,0,Some("C#"))
    val res = (raw.take(1)(0) == expectedPosting)
    assert(res, "rawPosting given samplePostings first value does not equal expected Posting")
  }


  test("'groupPostings' should create a grouped RDD of (K: posting ID, V: (question, Iterable[answers]))") {
    val lines = sc.parallelize(samplePostings)
    val raw = rawPostings(lines)
    val grouped = groupedPostings(raw)

    val sampleId = 9002525
    val expectedGroupRecord = List(
                                (Posting(1,9002525,None,None,2,Some("C++")),
                                 Posting(2,9003401,None,Some(9002525),4,None)),
                                (Posting(1,9002525,None,None,2,Some("C++")),
                                 Posting(2,9005311,None,Some(9002525),0,None))
                             )

    val resultGroupedRecord = grouped.filter(_._1 == sampleId).collect()(0)
    val res1 = resultGroupedRecord._1 == sampleId
    val res2 = resultGroupedRecord._2.toList == expectedGroupRecord
    assert(res1, "result from groupedPostings did not have the correct ID")
    assert(res2, "result grouping from groupedPostings was not the correct (question, answer) pairs")
  }

  test("'scoredPostings' should return the top score for each question") {
    val lines = sc.parallelize(samplePostings)
    val raw = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped)

    val expected = Array(
         (Posting(1,5484340,None,None,0,Some("C#")), 1),
         (Posting(1,9002525,None,None,2,Some("C++")), 4),
         (Posting(1,21984912,None,None,0,Some("Java")), 0),
         (Posting(1,9002524,None,None,2,None),4)
    )

    val scored_results = scored.collect

    scored_results.foreach {
      row => {
        val res = expected.contains(row)
        assert(res, "scoredPostings result did not contain expected results")
      }
    }

    val count_res = scored_results.size == expected.size
    assert(count_res, "result from scoredPostings did not have the correct number of records")
  }

  test("'vectorPostings' should return the vectored langauge and top score in prep for kmeans") {
    val lines = sc.parallelize(samplePostings)
    val raw = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped)
    val vectors = vectorPostings(scored)

    val expected = Array(
      (250000,4),
      (200000,1),
      (50000,0)
    )

    val vector_results = vectors.collect

    vector_results.foreach {
      row => {
        val res = expected.contains(row)
        assert(res, "vectorPostings result did not contain expected results")
      }
    }

    val count_res = vector_results.size == expected.size
    assert(count_res, "result from vectorPostings did not have the correct number of records")
  }
}
