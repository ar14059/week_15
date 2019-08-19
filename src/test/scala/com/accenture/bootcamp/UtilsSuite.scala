package com.accenture.bootcamp

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class UtilsSuite extends FunSuite with Matchers with BeforeAndAfterAll with SparkSupport {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)


  override def beforeAll(): Unit = {
    val cwd = new File(".")
      .getAbsolutePath
      .replaceAll("\\\\", "/")
      .replaceAll("/\\.$", "")

    val hadoopHomeDir = new File(cwd).toURI.toString
    sys.props += "hadoop.home.dir" -> cwd
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("Read SacramentocrimeJanuary2006 correctly") {
    val path = Utils.filePath("SacramentocrimeJanuary2006.csv")
    val rdd = sc.textFile(path)
    val count = rdd.count()
    assert(rdd.count() == 7584)
  }

  test("Read ucr_ncic_codes correctly") {
    val path = Utils.filePath("ucr_ncic_codes.tsv")
    val rdd = sc.textFile(path)
    val count = rdd.count()
    assert(rdd.count() == 563)
  }


  test("1) optimalCode1 and optimalCode11 optimization test") {
    def path_ucr_ncic_codes = Utils.filePath("ucr_ncic_codes.tsv")
    def rdd_ucr_ncic_codes = sc.textFile(path_ucr_ncic_codes)

    // Unoptimized function

    val t_start = System.nanoTime
    App.unoptimalCode1(rdd_ucr_ncic_codes)
    val t_duration = System.nanoTime - t_start

    val f1_res = App.unoptimalCode1(rdd_ucr_ncic_codes)

    // Fst optimized function

    val t_mod_1_start = System.nanoTime
    App.optimalCode1(rdd_ucr_ncic_codes)
    val t_mod_1_duration = System.nanoTime - t_mod_1_start

    val f2_res = App.optimalCode1(rdd_ucr_ncic_codes)
    val res_bool = f1_res._1.sameElements(f2_res._1) && f1_res._2.sameElements(f2_res._2)

    // Snd optimized function

    val t_mod_11_start = System.nanoTime
    App.optimalCode11(rdd_ucr_ncic_codes)
    val t_mod_11_duration = System.nanoTime - t_mod_11_start

    val f2_res_2 = App.optimalCode11(rdd_ucr_ncic_codes)
    val res_bool_2 = f1_res._1.sameElements(f2_res._1) && f2_res_2._2.sameElements(f2_res_2._2)

    // Compare speed AND function result for both functions

    assert(t_mod_1_duration < t_duration && t_mod_11_duration < t_duration && res_bool && res_bool_2)
  }



  test("2) Code2 optimization test") {
    def path_ucr_ncic_codes = Utils.filePath("ucr_ncic_codes.tsv")
    def rdd_ucr_ncic_codes = sc.textFile(path_ucr_ncic_codes)

    def path_SacramentocrimeJanuary2006 = Utils.filePath("SacramentocrimeJanuary2006.csv")
    def rdd_SacramentocrimeJanuary2006 = sc.textFile(path_SacramentocrimeJanuary2006)

    // Unoptimized function

    val folders = new File("output").listFiles.filter(_.isDirectory).length

    val t_start = System.nanoTime
    App.unoptimalCode2(rdd_ucr_ncic_codes, rdd_SacramentocrimeJanuary2006)
    val t_duration = System.nanoTime - t_start

    val folders_new = new File("output").listFiles.filter(_.isDirectory).length


    // optimized function

    val mod_folders = new File("output").listFiles.filter(_.isDirectory).length

    val t_mod_2_start = System.nanoTime
    App.optimalCode2(rdd_ucr_ncic_codes, rdd_SacramentocrimeJanuary2006)
    val t_mod_2_duration = System.nanoTime - t_mod_2_start

    val mod_folders_new = new File("output").listFiles.filter(_.isDirectory).length


    // Compare speed AND equal amount of added folders inside directory "output"

    assert(t_mod_2_duration < t_duration && folders_new - folders == mod_folders_new - mod_folders)
  }


}
