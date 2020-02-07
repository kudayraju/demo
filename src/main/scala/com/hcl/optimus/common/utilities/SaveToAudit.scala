package com.hcl.optimus.common.utilities

import com.hcl.optimus.common.log.logger.Logging
import org.json.JSONObject


object SaveToAudit extends Logging {

  def saveDataToAudit(job_id: String, job_name: String, lineage_key: String, job_exec_timestamp: String, success_count: Long, failure_count: Long): (Boolean) = {

    var passFalg = true
    try {
      val jsonObject = new JSONObject()

      jsonObject.put("job_id", job_id)
      jsonObject.put("job_name", job_name)
      jsonObject.put("lineage_key", lineage_key)
      jsonObject.put("job_exec_timestamp", job_exec_timestamp)
      jsonObject.put("success_count", success_count)
      jsonObject.put("failure_count", failure_count)
      jsonObject.put("total_process_count", success_count + failure_count)

      logCustomLevel("AUDITLOG", "", jsonObject.toString())

    } catch {
      case exp: Throwable => {
        exp.printStackTrace
      }
    }
    passFalg
  }

  def saveDataToAudit(job_id: String, job_name: String, lineage_key: String, job_start_timestamp: String, job_end_timestamp: String, success_count: Long, failure_count: Long): (Boolean) = {

    var passFalg = true
    try {
      val jsonObject = new JSONObject()

      jsonObject.put("job_id", job_id)
      jsonObject.put("job_name", job_name)
      jsonObject.put("lineage_key", lineage_key)
      jsonObject.put("job_start_time", job_start_timestamp)
      jsonObject.put("job_end_time", job_end_timestamp)
      jsonObject.put("success_count", success_count)
      jsonObject.put("failure_count", failure_count)
      jsonObject.put("total_process_count", success_count + failure_count)

      logCustomLevel("AUDITLOG", "", jsonObject.toString())

    } catch {
      case exp: Throwable => {
        exp.printStackTrace
      }
    }
    passFalg
  }


  def saveDataToKafka(row_key_prefix: String): (Boolean) = {
    var passFalg = true
    val jsonObject = new JSONObject()
    jsonObject.put("record", "A record")
    jsonObject.put("level", "DPERROR")
    jsonObject.put("message", "Rule failed while validating data")

    logCustomLevel("DPERROR", row_key_prefix, jsonObject.toString())

    passFalg
  }

  /*
   * Added by :: MrinalB
   * Added on :: 16-Jan-2019
   * Reason :: To have more columns to be saved as per new audit table structure
   */
  def saveDataToAudit(jobExecutionId: String, jobName: String, executionLineageKey: String, jobExecutionStart: String,
                      jobExecutionEnd: String, customAttribName: String, customAttribValue: String, processedBy: String,
                      record_read_count: Long, successRecordWriteCount: Long, failureRecordWriteCount: Long,
                      totalRecordCount: Long, byteReadCount: Long, byteWriteCount: Long, jobExecutionStatus: String,
                      statusDescription: String): (Boolean) = {

    var successFlag = false

    val jsonObject = new JSONObject()
    jsonObject.put("job_execution_id", jobExecutionId)
    jsonObject.put("job_name", jobName)
    jsonObject.put("execution_lineage_key", executionLineageKey)
    jsonObject.put("job_execution_start", jobExecutionStart)
    jsonObject.put("job_execution_end", jobExecutionEnd)
    jsonObject.put("custom_attrib_name", customAttribName)
    jsonObject.put("custom_attrib_value", customAttribValue)
    jsonObject.put("processed_by", processedBy)
    jsonObject.put("record_read_count", record_read_count)
    jsonObject.put("success_record_write_count", successRecordWriteCount)
    jsonObject.put("failure_record_write_count", failureRecordWriteCount)
    jsonObject.put("total_record_count", totalRecordCount)
    jsonObject.put("byte_read_count", byteReadCount)
    jsonObject.put("byte_write_count", byteWriteCount)
    jsonObject.put("job_execution_status", jobExecutionStatus)
    jsonObject.put("status_description", statusDescription)
    jsonObject.put("dummy_col", "dummy_col_value")

    println("Audit Log JSON :: " + jsonObject.toString)
    logCustomLevel("AUDITLOG", "", jsonObject.toString)

    successFlag = true
    successFlag
  }

  /*
   * Added by :: MrinalB
   * Added on :: 16-Jan-2019
   * Reason :: To have more columns to be saved as per new audit table structure
   */
  def saveDataToLineage(jobLineageKey: String, currentLineageKey: String, parentLineageKey: String): (Boolean) = {
    var successFlag = false

    val jsonObject = new JSONObject()
    jsonObject.put("job_lineage_key", jobLineageKey)
    jsonObject.put("current_lineage_key", currentLineageKey)
    jsonObject.put("parent_lineage_key", parentLineageKey)
    jsonObject.put("dummy_col", "dummy_col_value")

    println("Lineage Log JSON :: " + jsonObject.toString)
    logCustomLevel("LINEAGELOG", "", jsonObject.toString)

    successFlag = true
    successFlag
  }
}
