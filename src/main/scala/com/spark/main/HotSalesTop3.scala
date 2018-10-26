package com.spark.main


import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.spark.constant.Constants
import com.spark.dao.TaskDAO
import com.spark.domain.Task
import com.spark.util.{ParamUtils, SparkUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  */
object HotSalesTop3 {
    def main(args: Array[String]): Unit = {

        //1 创建 sparkSession
        val spark: SparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("HotSalesTop3")
                .enableHiveSupport()
                .getOrCreate()

        //注册两个自定义的函数
        spark.udf.register("concat", concatUDF)
        spark.udf.register("group_concat", new GroupConcatUDAF)
        spark.udf.register("getJsonObject", getJsonObjectUDF)

        //2 准备模拟数据
        SparkUtil.mockData(spark)

        //3 获取命令行传入的taskID, 从数据库中查询对应的任务
        val taskId: Int = args(0).toInt

        //4 根据 taskId 查询具体的任务
        val task: Task = TaskDAO.findById(taskId)

        //5 解析task的任务参数(json格式)
        val jsonObject: JSONObject = JSON.parseObject(task.getTaskParam)

        //6 解析出指定日期范围
        val statDate: String = ParamUtils.getParam(jsonObject, Constants.PARAM_START_DATE)
        val endDate: String = ParamUtils.getParam(jsonObject, Constants.PARAM_END_DATE)

        //7 过滤出指定日期范围内的用户点击行为
        //page_id, city_id, pay_category_ids, order_category_ids, click_product_id, action_time, date, search_keyword, pay_product_ids, order_product_ids, session_id, user_id, click_category_id]
        val sql: String =
            s"""
              |select city_id, click_product_id as product_id
              |from user_visit_action
              |where click_product_id is not NULL
              |and action_time >= '$statDate'
              |and action_time <= '$endDate'
            """.stripMargin

        val clickActionDF: DataFrame = spark.sql(sql)

        clickActionDF.show(10)

        //8 丛MySQL数据库中获取city数据
        val props: Properties = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "123")
        val cityDF: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/spark_project", "city_info", props)
        cityDF.show(10)

        //9 将点击行为数据和city信息进行
        val actionAndCityDF: DataFrame = clickActionDF.join(cityDF, Seq("city_id"))
        actionAndCityDF.show(10)

        //10 将结果df注册为一个临时表 tem_clk_prod_basic
        actionAndCityDF.createOrReplaceTempView("tmp_clk_prod_basic")

        //11 生成各区域商品点击次数临时表
        // 按照 area, product_id 进行分组, 可以获得每个area下的每个product_id的城市信息拼接起来的串
        // 可以计算出各区域个商品的点击次数
        val sql2: String =
            """
              |select area, product_id, count(1) as click_count, group_concat(concat(city_id, city_name, ':')) as city_infos
              |from tmp_clk_prod_basic
              |group by area, product_id
            """.stripMargin

        spark.sql(sql2).createOrReplaceTempView("tmp_area_prod_clk_cnt")

//        val df2: DataFrame = spark.sql(sql2)
//        df2.show()

        //12 join商品表, 获取商品的完整信息
        val sql3: String =
            """
              |select area, tapcc.product_id as product_id, click_count, city_infos, product_name,
              |     if(getJsonObject(extend_info, 'product_status') = '0', '自营', '第三方') as product_status
              |from tmp_area_prod_clk_cnt tapcc
              | join product_info pi on tapcc.product_id = pi.product_id
            """.stripMargin

        spark.sql(sql3).createOrReplaceTempView("tmp_area_fullprod_clk_cnt")

        //13 统计热门商品top3
        // 华北, 华东, 华南, 华中, 西北, 西南, 东北
        // A 级: 华北, 华东
        // B 级: 华南, 华中
        // C 级: 西北, 西南
        // D 级: 东北
        val sql4 =
            s"""
              |select $taskId as task_id, area,
              |     case
              |         when area = '华北' or area = '华东' then 'A 级'
              |         when area = '华南' or area = '华中' then 'B 级'
              |         when area = '西北' or area = '西南' then 'C 级'
              |         else 'D 级'
              |      end as area_level,
              |     product_id, click_count, city_infos, product_name, product_status
              |from (
              |       select area, product_id, click_count, city_infos, product_name, product_status,
              |          row_number() over (partition by area order by click_count desc) as rank
              |       from tmp_area_fullprod_clk_cnt
              |       ) t
              |where t.rank <= 3
            """.stripMargin

        val hotSalesTop3: DataFrame = spark.sql(sql4)

        //14 将数据写入到MySql
        hotSalesTop3.write.mode("append").jdbc("jdbc:mysql://localhost:3306/spark_project", "area_top3_product", props)

        spark.stop()
    }

    /**
      * 自定义UDF: 拼接字段
      */
    val concatUDF : (Long, String, String) => String = (id, name, delimated) => id + delimated + name

    /**
      * 自定义UDF函数: 从json 字符串中获取
      *
      * json 数据格式: "{\"product_status\": " + productStatus[random.nextInt(2)] + "}"
      */
    val getJsonObjectUDF: (String, String) => String = (json, field) => {
        try {
            val parseObject: JSONObject = JSON.parseObject(json)
            val value: String = parseObject.getString(field)

            value
        } catch {
            case e: Exception => println(e)
        }
        null
    }
}
