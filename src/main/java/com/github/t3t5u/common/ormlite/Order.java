package com.github.t3t5u.common.ormlite;

import java.util.Date;

import com.github.t3t5u.common.database.Entity;
import com.j256.ormlite.stmt.QueryBuilder;

public interface Order<E extends Entity> {
	QueryBuilder<E, Long> orderBy(QueryBuilder<E, Long> queryBuilder, Date now);
}
