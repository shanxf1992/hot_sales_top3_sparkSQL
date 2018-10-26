package com.spark.dao

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.spark.domain.Task
import com.spark.util.JDBCUtils

object TaskDAO {

    def findById(taskId: Int): Task= {

         //1 获取数据连接池
         val connection: Connection = JDBCUtils.getConnection

         val sql: String =
             s"""
               |select *
               |from task
               |where task_id = $taskId
             """.stripMargin

         val ps: PreparedStatement = connection.prepareStatement(sql)

        val rs: ResultSet = ps.executeQuery()

        val task: Task = new Task()
        if (rs.next()) {
            val task_id: Int = rs.getInt("task_id")
            val task_param: String = rs.getString("task_param")
            task.setTaskId(task_id.toLong)
            task.setTaskParam(task_param)
        }
        task
    }

}
