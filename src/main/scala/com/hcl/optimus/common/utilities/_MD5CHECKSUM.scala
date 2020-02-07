package com.hcl.optimus.common.utilities

import java.security.MessageDigest

import org.apache.spark.internal.Logging

@SerialVersionUID(100L)
sealed class _MD5CHECKSUM() extends Serializable with Logging {

  def getMD5CheckSumForSingleData(text: String): String = {
    val data = MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
    data
  }

  def getMD5CheckSum(input: String*): String = {

    var data: String = ""
    try {

      val iterator = input.iterator
      val stringBuffer = new StringBuffer();
      for (str <- iterator) {
        stringBuffer.append(str.asInstanceOf[String]);
      }

      val md = MessageDigest.getInstance("MD5");
      md.update(stringBuffer.toString().getBytes());
      val digest = md.digest()
      data = digest.map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }

    } catch {
      case t: Throwable => { //t.printStackTrace() // TODO: handle error
        logError(t.getMessage())
        throw new Exception(t);
      }
    }

    data
  }

}

object md5checksum extends _MD5CHECKSUM()

object TestMain {

  def main(args: Array[String]): Unit = {
    /*println(md5checksum.getMD5CheckSum("I am", "John"))
    println(md5checksum.getMD5CheckSum("I am", "John"))
    println(md5checksum.getMD5CheckSum("I am", "Joe"))
    println(md5checksum.getMD5CheckSum("I am John"))
    println(md5checksum.getMD5CheckSumForSingleData("I am John"))*/
  }
}