package com.spark.util

import com.alibaba.fastjson.{JSONArray, JSONObject}

object ParamUtils {

    /**
      * 从JSON对象中提取参数
      *
      * @param jsonObject JSON对象
      * @return 参数
      */
    def getParam(jsonObject: JSONObject, field: String) = {
        val array: JSONArray = jsonObject.getJSONArray(field)
        if(array != null && array.size() > 0) {
            array.getString(0)
        }else{
            null
        }
    }

}
