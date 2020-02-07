package com.hcl.optimus.common.utilities

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import sun.misc.{BASE64Decoder, BASE64Encoder}


object EncryptionDecryptionUtils{
  val KeyValue : Array[Byte] = Array[Byte]('T', 'h', 'i', 's', 'I', 's', 'A', 'S', 'e', 'c', 'r', 'e', 't', 'K', 'e', 'y')
  val Iteration : Int = 2
  val ALGORITHM : String = "AES"
  val SALT = "$%TDEFDFT$^^&#$RCVHGYT^&GHDEE#EDCVFR$^TGFVCDRE#$*&HTR$DDW##DEW"

  def generateKey: SecretKeySpec = new SecretKeySpec(KeyValue, ALGORITHM)

  def getCipher( algo : Int ): Cipher = {
    val cipher = Cipher.getInstance(ALGORITHM)
    cipher.init(algo, generateKey)
    cipher
  }
}

trait Encrypt[T, R] extends (T => R)
object Encrypt extends Encrypt[String, String]{
  import EncryptionDecryptionUtils.{Iteration, SALT, getCipher}
  override def apply( value : String): String = {
    val cipher = getCipher(Cipher.ENCRYPT_MODE)
    var valueToEnc : String= null
    var eValue = value
    for( i<- 0 until Iteration){
      valueToEnc = SALT + eValue
      val encValue = cipher.doFinal(valueToEnc.getBytes())
      eValue = new BASE64Encoder().encode(encValue)
    }
    eValue
  }
}


trait Decrypt[T, R] extends (T => R)
object Decrypt extends Decrypt[String, String] {
  import EncryptionDecryptionUtils.{Iteration, SALT, getCipher}
  override def apply(value: String): String = {
    val cipher = getCipher(Cipher.DECRYPT_MODE)
    var decryptValue : String = null
    var valueToDecrypt : String = value
    for( i <- 0 until Iteration){
      val decodedValue = new BASE64Decoder().decodeBuffer(valueToDecrypt)
      decryptValue = new String(cipher.doFinal(decodedValue)).substring(SALT.length)
      valueToDecrypt = decryptValue
    }
    valueToDecrypt
  }
}


object AESEncryptDecrypt {

  def main(args: Array[String]): Unit = {
    val encryptOrDecrypt = args(0)
    val valueToOperation = args(1)

    val result: String = encryptOrDecrypt match {
      case s if "Encrypt".equalsIgnoreCase(s) => Encrypt(valueToOperation)
      case s if "Decrypt".equalsIgnoreCase(s) => Decrypt(valueToOperation)
      case _ => throw new Exception(" Argument 0 should be Enctypt/Decrypt and Argument 1 should be value for the operation")
    }

    println(s"Operation : $encryptOrDecrypt, given value : $valueToOperation, result : $result")
  }
}
