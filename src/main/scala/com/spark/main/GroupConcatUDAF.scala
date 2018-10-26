package com.spark.main

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 自定义UDAF函数, 实现自定义聚合
  */
class GroupConcatUDAF extends UserDefinedAggregateFunction{
    /**
      * 指定具体的输入数据的类型: 名称, 类型, 是否可以为空
      */
    override def inputSchema: StructType = StructType(Array(StructField("cityInfo", StringType, true)))

    /**
      * 在进行聚合操作的时候所要处理的数据的中间结果类型
      */
    override def bufferSchema: StructType = StructType(Array(StructField("bufferCityInfo", StringType, true)))

    /**
      * 指定返回值的类型
      */
    override def dataType: DataType = StringType

    /**
      * 指定是否是确定性的
      */
    override def deterministic: Boolean = true

    /**
      * 初始化, 可以认为是能够在内部指定一个初始的值
      * @param buffer
      */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer.update(0, "")
    }

    /**
      * 更新操作, 可以认为是将组内的字段一个一个的传递进来, 自己来实现拼接的逻辑
      */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 缓冲中的已经拼接过的城市信息串
        var bufferCityInfo: String = buffer.getString(0)
        // 刚刚传递进来的某个城市信息
        val cityInfo: String = input.getString(0)

        //实现去重的逻辑, 之前没有拼接过的某个城市信息, 那么这里才可以接下去拼接新的城市信息
        if (!bufferCityInfo.contains(cityInfo)) {
            if ("".equals(bufferCityInfo)) {
                bufferCityInfo = bufferCityInfo + cityInfo
            } else {
                // 例如, 1:北京 --> 1:北京,2:上海
                bufferCityInfo = bufferCityInfo + "," + cityInfo
            }
            // 将 bufferCityInfo 更新到缓存当中
            buffer.update(0, bufferCityInfo)
        }
    }

    /**
      * 合并操作, 将不同的buffer中的内容进行合并, 因为 update 可能是针对一个分组内的部分数据, 在某个节点上发生的, 但是一个分组内的数据可能分布在多个分组上
      */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val cityInfo1: String = buffer1.getString(0) // 1:北京,2:上海,3:南京
        var cityInfo2: String = buffer2.getString(0) // 2:上海,4:深圳,5:广州

        // 将两个城市信息进行合并, 并且去重
        val cities: Array[String] = cityInfo1.split(",")
        for (city <- cities) {
            // 如果当前城市信息, 不存在cityInfo2中就进行拼接
            if (!cityInfo2.contains(city)) {
                // 判断cityInfo2是否为空, 即没有拼接过
                if("".equals(cityInfo2)) {
                    cityInfo2 = city
                } else {
                    cityInfo2 = cityInfo2 + "," + city
                }
            }
        }
        // 将cityinfo2更新到缓存
        buffer1.update(0, cityInfo2)
    }

    /**
      *返回最终的结果
      */
    override def evaluate(buffer: Row): Any = {
        buffer.getString(0)
    }
}
