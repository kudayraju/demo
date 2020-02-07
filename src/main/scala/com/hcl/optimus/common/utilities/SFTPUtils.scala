package com.hcl.optimus.optimus.common.utilities

import java.io.{File, FilenameFilter}

import com.jcraft.jsch.{Channel, ChannelSftp, JSch, Session}

import scala.collection.mutable.ListBuffer
import com.hcl.optimus.common.model.ConnectionDetails

object SFTPUtils {
  private var jsch: JSch = _
  private var session: Session = _
  private var channel: Channel = _
  private var sftpChannel: ChannelSftp = _

  // Connection module
  private def connect(sftpDetails: ConnectionDetails): Unit = {
    println("connecting..." + sftpDetails.host)
    jsch = new JSch()
    session = jsch.getSession(sftpDetails.user, sftpDetails.host, sftpDetails.port)
    session.setConfig("StrictHostKeyChecking", "no") // has to be parameterized
    session.setPassword(sftpDetails.password)
    session.connect()
    channel = session.openChannel("sftp")
    channel.connect()
    sftpChannel = channel.asInstanceOf[ChannelSftp]
  }

  // Disconnection Module
  private def disconnect(): Unit = {
    //println("disconnecting..." + sftpDetails.host)
    sftpChannel.disconnect()
    channel.disconnect()
    session.disconnect()
  }

  //List all files
  private def _listFiles(sftpDetails: ConnectionDetails, remotePath: String): List[String] ={
    connect(sftpDetails)
    val file: File = new File(remotePath)
    val files: Array[File] = file.listFiles
    val fileList = new ListBuffer[String]
    for (f1 <- files) {
      fileList.append(f1.getName)
    }
    disconnect
    fileList.toList
  }

  // List files according to File specific file pattern
  def listFiles(sftpDetails: ConnectionDetails, remotePath: String, fileNamePattern: Option[String]): List[String] = {
    fileNamePattern match {
      case None => _listFiles(sftpDetails,remotePath)
      case _ =>
        connect(sftpDetails)
        val pat = fileNamePattern.toString.split("\\*")
        val file: File = new File(remotePath)
        val files: Array[File] = file.listFiles(new FilenameFilter {
          override def accept(dir: File, name: String): Boolean =
            if ((name.startsWith(pat(0))) && (name.endsWith(pat(1)))) {
              true
            } else {
              false
            }
        })
        val fileList = new ListBuffer[String]
        for (f1 <- files) {
          fileList.append(f1.getName)
        }
        disconnect
        fileList.toList
    }
  }

  // Download Module
  //Download files according to a specific pattern
  private def _downloadFile(sftpDetails: ConnectionDetails, remotePath: String, remoteFileName: String, localPath: String): Unit = {
    connect(sftpDetails)
    sftpChannel.get(remotePath, localPath)
    println("File downloaded successfully - " + remoteFileName)
    disconnect
  }

  def downloadFiles(sftpDetails: ConnectionDetails, remotePath: String, remoteFileList: List[String], localPath: String): Unit ={
    remoteFileList foreach { file =>
      _downloadFile(sftpDetails,remotePath,file,localPath)
    }
  }

  def countRecords(sftpDetails: ConnectionDetails, remotePath: String, remoteFile: String): Long ={
    0.toLong
    //implement
  }
}
