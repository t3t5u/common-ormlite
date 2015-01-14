package com.github.t3t5u.common.ormlite;

import java.util.Date;

import com.github.t3t5u.common.database.Entity;
import com.j256.ormlite.dao.Dao;

public interface Database {
	Date getNow();

	<E extends Entity> Dao<E, Long> getDao(Class<E> clazz);
}
