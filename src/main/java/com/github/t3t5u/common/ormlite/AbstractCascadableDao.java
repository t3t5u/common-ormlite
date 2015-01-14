package com.github.t3t5u.common.ormlite;

import java.util.List;

import com.github.t3t5u.common.database.CascadableDao;
import com.github.t3t5u.common.database.DatabaseUtils;
import com.github.t3t5u.common.database.Entity;
import com.j256.ormlite.stmt.QueryBuilder;

public abstract class AbstractCascadableDao<E extends Entity> extends AbstractDao<E> implements CascadableDao<E> {
	protected AbstractCascadableDao(final Database database, final Class<E> clazz) {
		super(database, clazz);
	}

	protected E find(final QueryBuilder<E, Long> queryBuilder, final boolean cascade) {
		return cascade ? DatabaseUtils.cascade(this, find(queryBuilder)) : find(queryBuilder);
	}

	@Override
	public List<E> findAll(final boolean cascade) {
		return cascade ? DatabaseUtils.cascade(this, findAll()) : findAll();
	}

	protected List<E> findAll(final QueryBuilder<E, Long> queryBuilder, final boolean cascade) {
		return cascade ? DatabaseUtils.cascade(this, findAll(queryBuilder)) : findAll(queryBuilder);
	}

	@Override
	public long deleteAll(final boolean cascade) {
		return DatabaseUtils.deleteAll(this, findAll(), cascade);
	}

	protected long deleteAll(final QueryBuilder<E, Long> queryBuilder, final boolean cascade) {
		return DatabaseUtils.deleteAll(this, findAll(queryBuilder), cascade);
	}
}
