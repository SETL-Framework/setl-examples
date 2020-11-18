package com.github.joristruong.utils

import com.jcdecaux.setl.annotation.ColumnName


case class Grade(date: String,
                 @ColumnName("name") pName: String,
                 grade: Int)
