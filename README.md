# hot_sales_top3_sparkSQL
##### 需求: 根据用户指定的日期范围, 统计各个区域下的热门商品top3 (SparkSQL)

​	spark作业接收taskId, 查询对应的Mysql的task, 获取用户指定的参数, 统计指定日期范围内的, 各个区域的top3 热门商品, 最后将结果写入到MySQL中

##### 技术方案

1. 查询task, 获取日期范围, 通过 sparkSQl, 查询 user_visit_action 表中指定日期范围内的数据, 过滤出商品点击的行为, click_product_id is not null; click_product_id != null;  click_product_id != null; city_id, click_product_id;
2. 使用spark SQL 从mysql 中查询出来城市信息(city_id, city_name, area), 用户访问行为数据要跟城市信息进行 join, city_id, city_name, area, product_id, 将RDD 转成DF, 并注册为临时表
3. SparkSQL 内置函数, case when , 对每个 area 打上一个标记
4. 计算出来每个区域下每个商品的点击次数, 根据 group by area, product_id, 保留每个区域的城市名称列别; 通过自定义UDF, group_contact_distinct() 函数, 聚合出来一个city_names 字段, area, product_id, city_names, click_count
5. join 商品明细表, hive( product_id, product_name, extend_info), extend_info 是一个 json 类型, 需要自定义UDF, get_json_object() 函数, 取出其中的 product_status 字段, if() 函数判断, 0 自营, 1 第三方. (area,  product_id, city_names, click_count, product_name, product_status)
6. 开窗函数, 根据 area 聚合, 获取每个 area 下, click_count 排名 前3 的product 信息; (area,  product_id, city_names, click_count, product_name, product_status)
7. 结果写入MySQL中
8. 解决sparkSQL 的数据倾斜解决方案(**双重groupBy,  随机Key进行扩容,  Spark内置的 reduce join 转 map join,  shuffle 并行度**)

##### 基础数据

1. Mysql 表要有 city_info(city_info, city_name, area)
2. Hive 表, product_info (product_id, product_name, extend_info)
3. MySql 表, 结果表 task_id, area, area_level, product_id, city_names, click_count, product_name, product_status 