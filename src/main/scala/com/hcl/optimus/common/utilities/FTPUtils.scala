package com.hcl.optimus.optimus.common.utilities

import java.io.{File, FileOutputStream, InputStream}

import com.hcl.optimus.common.model.ConnectionDetails
import org.apache.commons.net.ftp.{FTPClient, FTPFile, FTPFileFilter}

import scala.collection.mutable.ListBuffer

object FTPUtils {
  //Connection Module
  private def connect(ftpDetails: ConnectionDetails): FTPClient ={
    val client = new FTPClient()
    println("connecting..." + ftpDetails.host)
    client.connect(ftpDetails.host, ftpDetails.port)
    client.login(ftpDetails.user, ftpDetails.password)
    client.enterLocalPassiveMode()
    client
  }

  //Listing Module
  //List all files
  private def _listFiles(ftpDetails: ConnectionDetails, remotePath: String) ={
    val fTPClient = connect(ftpDetails)
    val result: Array[FTPFile] = fTPClient.listFiles(remotePath)
    val fileList = new ListBuffer[String]
    for(f1 <- result){
      fileList.append(f1.getName)
    }
    fTPClient.disconnect()
    fileList.toList
  }

  //List files according to a specific pattern
  def listFiles(ftpDetails: ConnectionDetails, remotePath: String, fileNamePattern: Option[String]): List[String] = {
    fileNamePattern match {
      case None => _listFiles(ftpDetails,remotePath)
      case _ =>
        val fTPClient = connect(ftpDetails)
        val pat = fileNamePattern.toString.split("\\*")
        val filter: FTPFileFilter = new FTPFileFilter() {
          override def accept(ftpFile: FTPFile): Boolean =
            (ftpFile.getName.startsWith(pat(0)) && ftpFile.getName.endsWith(pat(1)))
        }
        val result: Array[FTPFile] = fTPClient.listFiles(remotePath, filter)
        val fileList = new ListBuffer[String]
        for(f1 <- result){
          fileList.append(f1.getName)
        }
        fTPClient.disconnect()
        fileList.toList
    }
  }

  // Download Module
  //Download according to specific pattern
  private def _downloadFile (ftpDetails: ConnectionDetails, remotePath: String, remoteFileName: String, localPath: String): Unit = {
    val fTPClient = connect(ftpDetails)
    val os = new FileOutputStream(new File(localPath + "/" + remoteFileName))
    fTPClient.retrieveFile(remotePath,os)
    println("File downloaded successfully - " + remotePath)
    fTPClient.disconnect()
  }

  //Download all files
  def downloadFiles (ftpDetails: ConnectionDetails, remotePath: String, remoteFileList: List[String], localPath: String): Unit = {
    remoteFileList foreach { file =>
      _downloadFile(ftpDetails, remotePath, file, localPath)
    }
  }
  def uploadFiles (ftpDetails: ConnectionDetails, remotePath: String,  local: InputStream ): Unit = {
    val fTPClient = connect(ftpDetails)
    fTPClient.storeFile(remotePath, local)

  }
  def countRecords(ftpDetails: ConnectionDetails, remotePath: String, remoteFile: String): Long ={
    0.toLong
    //implement
  }
}
