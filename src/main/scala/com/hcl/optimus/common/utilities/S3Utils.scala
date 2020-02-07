package com.hcl.optimus.common.utilities

import java.io.{BufferedReader, InputStreamReader}
import java.text.SimpleDateFormat

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model.{GetObjectRequest, ListObjectsRequest}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object S3Utils {
  def getMaxDatetimeFolderName(bucketName: String, basePath: String, awsaccesskeyid : String, awssecretaccesskey : String) : String = {

    val folders = getAllFolders(bucketName, basePath, awsaccesskeyid, awssecretaccesskey)
 
    var allFolderNamesAndTimeMap = scala.collection.mutable.Map[Long, String]()
    folders.foreach(fullName => allFolderNamesAndTimeMap += (getEpochTimeFromFolderName(fullName) -> fullName))
 
    val highestEpochTime = allFolderNamesAndTimeMap.keySet.toList.max
 
    val lastFolderFullname = allFolderNamesAndTimeMap.get(highestEpochTime).get
 
    val names = lastFolderFullname.split("[/]")
    val name = names(names.length - 1)
    name
  }
  def isLeafFolder(bucketName: String, basePath: String, awsaccesskeyid : String, awssecretaccesskey : String) : Boolean = {
    val credentials = new BasicAWSCredentials(awsaccesskeyid, awssecretaccesskey);
    val s3Client = new AmazonS3Client(credentials);
    val lor: ListObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(basePath).withDelimiter("/");
    val folders = s3Client.listObjects(lor).getCommonPrefixes
    folders.size() match {
      case 0 => true
      case _ => false
    }
  }
  def writeToS3(bucketName: String, pathWithFileName: String, awsaccesskeyid : String, awssecretaccesskey : String, data: String) = {
    import com.amazonaws.auth.BasicAWSCredentials
    val credentials = new BasicAWSCredentials(awsaccesskeyid, awssecretaccesskey)
    import com.amazonaws.services.s3.AmazonS3Client
    val s3Client = new AmazonS3Client(credentials)
    s3Client.putObject(bucketName, pathWithFileName, data)

  }
  
  def doesObjectExist(bucketName: String, path: String, awsaccesskeyid : String, awssecretaccesskey : String) : Boolean = {
    val credentials = new BasicAWSCredentials(awsaccesskeyid, awssecretaccesskey);
    val s3Client = new AmazonS3Client(credentials);
    val objectSize = s3Client.listObjectsV2(bucketName, path).getObjectSummaries.size
    objectSize match {
      case 0 => false
      case _ => true
    }
  }
  
  def getLastReadIndicatorFromS3(bucketName: String, pathWithFileName: String, awsaccesskeyid : String, awssecretaccesskey : String) : String = {
    try {
      import com.amazonaws.auth.BasicAWSCredentials
      val credentials = new BasicAWSCredentials(awsaccesskeyid, awssecretaccesskey)
      import com.amazonaws.services.s3.AmazonS3Client
      val s3Client = new AmazonS3Client(credentials)
      import com.amazonaws.services.s3.model.GetObjectRequest
      val objectContent = s3Client.getObject(new GetObjectRequest(bucketName, pathWithFileName)).getObjectContent
      val reader = new BufferedReader(new InputStreamReader(objectContent))
      reader.readLine()
    }catch {
      case _ : Throwable=> throw new Exception(s"Problem with File $pathWithFileName - may not be present!")
    }
  }
  def getAllFolders(bucketName: String, basePath: String, awsaccesskeyid : String, awssecretaccesskey : String) : List[String] = {
    val credentials = new BasicAWSCredentials(awsaccesskeyid, awssecretaccesskey);
    val s3Client = new AmazonS3Client(credentials);
    val lor: ListObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(basePath).withDelimiter("/");
    val folders = s3Client.listObjects(lor).getCommonPrefixes
    println(s"getAllFolders : $folders")
    folders.asScala.toList
  }
  def getEpochTimeFromFolderName(folderName: String) : Long = {
    val names = folderName.split("[/]")
    val name = names(names.length - 1).replaceAll("-", "")+"00"
    println(s"Name to convert to time : $name")
    val sdf = new SimpleDateFormat("yyyyMMddhhmmss")
    sdf.parse(name).getTime
  }
  def isFolderInRange(folderName: String, startDatetime: Long, endDatetime: Long) : Boolean = {
    val epochTime = getEpochTimeFromFolderName(folderName)
    if(epochTime > startDatetime && epochTime <= endDatetime)
      true
    else
      false
  }
  def getSelectedFolders(bucketName: String, basePath: String, awsaccesskeyid : String, awssecretaccesskey : String,
                                     startDatetime: String, endDatetime: String) : ListBuffer[String] = {
    val selectedFolders : ListBuffer[String] = new ListBuffer[String]

    val folders = getAllFolders(bucketName, basePath, awsaccesskeyid, awssecretaccesskey)
    val sdf = new SimpleDateFormat("yyyyMMddhhmmss")
    val startDateTimeWithSecond = startDatetime + "-00"
    val endDateTimeWithSecond = endDatetime + "-00"
    val startEpochTime = sdf.parse(startDateTimeWithSecond.replaceAll("-", "").replaceAll(" ", "")).getTime
    val endEpochTime = sdf.parse(endDateTimeWithSecond.replaceAll("-", "").replaceAll(" ", "")).getTime
    println(s"startEpochTime : $startEpochTime")
    val selectedFolder = folders.filter(isFolderInRange(_, startEpochTime, endEpochTime))
    selectedFolder.to[ListBuffer]
  }
}
