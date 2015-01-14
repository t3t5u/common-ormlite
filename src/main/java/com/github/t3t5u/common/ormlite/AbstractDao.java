package com.github.t3t5u.common.ormlite;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.t3t5u.common.database.Entity;
import com.github.t3t5u.common.database.Transactional;
import com.github.t3t5u.common.database.WhereVisitor;
import com.github.t3t5u.common.expression.Expression;
import com.j256.ormlite.dao.BaseDaoImpl;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.Dao.CreateOrUpdateStatus;
import com.j256.ormlite.stmt.DeleteBuilder;
import com.j256.ormlite.stmt.PreparedDelete;
import com.j256.ormlite.stmt.PreparedQuery;
import com.j256.ormlite.stmt.PreparedUpdate;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.stmt.StatementBuilder;
import com.j256.ormlite.stmt.UpdateBuilder;
import com.j256.ormlite.support.DatabaseConnection;
import com.j256.ormlite.table.TableInfo;

public abstract class AbstractDao<E extends Entity> implements com.github.t3t5u.common.database.Dao<E>, Transactional {
	private static final ThreadLocal<DatabaseConnection> CONNECTION = new ThreadLocal<DatabaseConnection>();
	private static final WhereVisitor WHERE_VISITOR = new WhereVisitor();
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDao.class);
	private final Database database;
	private final Dao<E, Long> dao;

	protected AbstractDao(final Database database, final Class<E> clazz) {
		this.database = database;
		this.dao = database.getDao(clazz);
	}

	@Override
	public E find(final long id) {
		try {
			return find(dao, id);
		} catch (final Throwable t) {
			LOGGER.info("find", t);
			return null;
		}
	}

	@Override
	public List<E> findAll() {
		try {
			return findAll(dao);
		} catch (final Throwable t) {
			LOGGER.info("findAll", t);
			return Collections.emptyList();
		}
	}

	@Override
	public boolean insert(final E entity) {
		if (entity == null) {
			return false;
		}
		final Date now = getNow();
		try {
			return insert(dao, entity, now);
		} catch (final Throwable t) {
			LOGGER.info("insert", t);
			return false;
		}
	}

	@Override
	public boolean insertOrUpdate(final E entity) {
		if (entity == null) {
			return false;
		}
		final Date now = getNow();
		try {
			return insertOrUpdate(dao, entity, now);
		} catch (final Throwable t) {
			LOGGER.info("insertOrUpdate", t);
			return false;
		}
	}

	@Override
	public boolean update(final E entity) {
		if (entity == null) {
			return false;
		}
		final Date now = getNow();
		try {
			return update(dao, entity, now);
		} catch (final Throwable t) {
			LOGGER.info("update", t);
			return false;
		}
	}

	@Override
	public boolean delete(final E entity) {
		if (entity == null) {
			return false;
		}
		try {
			return delete(dao, entity);
		} catch (final Throwable t) {
			LOGGER.info("delete", t);
			return false;
		}
	}

	@Override
	public long deleteAll() {
		try {
			return deleteAll(dao);
		} catch (final Throwable t) {
			LOGGER.info("deleteAll", t);
			return 0;
		}
	}

	@Override
	public long count() {
		try {
			return count(dao);
		} catch (final Throwable t) {
			LOGGER.info("count", t);
			return 0;
		}
	}

	@Override
	public void begin() {
		if (CONNECTION.get() != null) {
			throw new IllegalStateException();
		}
		final DatabaseConnection connection;
		try {
			connection = begin(dao);
		} catch (final SQLException e) {
			LOGGER.warn("begin", e);
			throw new RuntimeException(e);
		}
		CONNECTION.set(connection);
	}

	@Override
	public void commit() {
		final DatabaseConnection connection = CONNECTION.get();
		if (connection == null) {
			throw new IllegalStateException();
		}
		try {
			commit(dao, connection);
		} catch (final SQLException e) {
			LOGGER.warn("commit", e);
			throw new RuntimeException(e);
		}
		CONNECTION.remove();
	}

	@Override
	public void rollback() {
		final DatabaseConnection connection = CONNECTION.get();
		if (connection == null) {
			return;
		}
		CONNECTION.remove();
		try {
			rollback(dao, connection);
		} catch (final SQLException e) {
			LOGGER.warn("rollback", e);
			throw new RuntimeException(e);
		}
	}

	protected E find(final QueryBuilder<E, Long> queryBuilder) {
		if (queryBuilder == null) {
			return null;
		}
		try {
			return find(dao, queryBuilder);
		} catch (final Throwable t) {
			LOGGER.info("find", t);
			return null;
		}
	}

	protected List<E> findAll(final QueryBuilder<E, Long> queryBuilder) {
		if (queryBuilder == null) {
			return Collections.emptyList();
		}
		try {
			return findAll(dao, queryBuilder);
		} catch (final Throwable t) {
			LOGGER.info("findAll", t);
			return Collections.emptyList();
		}
	}

	protected long updateAll(final UpdateBuilder<E, Long> updateBuilder) {
		if (updateBuilder == null) {
			return 0;
		}
		final Date now = getNow();
		try {
			return updateAll(dao, updateBuilder, now);
		} catch (final Throwable t) {
			LOGGER.info("updateAll", t);
			return 0;
		}
	}

	protected long deleteAll(final DeleteBuilder<E, Long> deleteBuilder) {
		if (deleteBuilder == null) {
			return 0;
		}
		try {
			return deleteAll(dao, deleteBuilder);
		} catch (final Throwable t) {
			LOGGER.info("deleteAll", t);
			return 0;
		}
	}

	protected long count(final QueryBuilder<E, Long> queryBuilder) {
		if (queryBuilder == null) {
			return 0;
		}
		try {
			return count(dao, queryBuilder);
		} catch (final Throwable t) {
			LOGGER.info("count", t);
			return 0;
		}
	}

	protected QueryBuilder<E, Long> buildQuery() {
		return dao.queryBuilder();
	}

	protected QueryBuilder<E, Long> buildQuery(final Order<E> order) {
		final Date now = getNow();
		return buildQuery(new ThrowableQueryBuilder<E>() {
			@Override
			public QueryBuilder<E, Long> buildQuery(final QueryBuilder<E, Long> queryBuilder) throws SQLException {
				return AbstractDao.buildQuery(queryBuilder, null, null, now, order);
			}
		});
	}

	@SuppressWarnings("unchecked")
	protected QueryBuilder<E, Long> buildQuery(final Order<E>... orders) {
		final Date now = getNow();
		return buildQuery(new ThrowableQueryBuilder<E>() {
			@Override
			public QueryBuilder<E, Long> buildQuery(final QueryBuilder<E, Long> queryBuilder) throws SQLException {
				return AbstractDao.buildQuery(queryBuilder, null, null, now, orders);
			}
		});
	}

	protected QueryBuilder<E, Long> buildQuery(final Expression<Boolean> expression) {
		return buildQuery(new ThrowableQueryBuilder<E>() {
			@Override
			public QueryBuilder<E, Long> buildQuery(final QueryBuilder<E, Long> queryBuilder) throws SQLException {
				return AbstractDao.buildStatement(queryBuilder, expression, null);
			}
		});
	}

	protected QueryBuilder<E, Long> buildQuery(final Expression<Boolean> expression, final Order<E> order) {
		final Date now = getNow();
		return buildQuery(new ThrowableQueryBuilder<E>() {
			@Override
			public QueryBuilder<E, Long> buildQuery(final QueryBuilder<E, Long> queryBuilder) throws SQLException {
				return AbstractDao.buildQuery(queryBuilder, expression, null, now, order);
			}
		});
	}

	@SuppressWarnings("unchecked")
	protected QueryBuilder<E, Long> buildQuery(final Expression<Boolean> expression, final Order<E>... orders) {
		final Date now = getNow();
		return buildQuery(new ThrowableQueryBuilder<E>() {
			@Override
			public QueryBuilder<E, Long> buildQuery(final QueryBuilder<E, Long> queryBuilder) throws SQLException {
				return AbstractDao.buildQuery(queryBuilder, expression, null, now, orders);
			}
		});
	}

	protected QueryBuilder<E, Long> buildQuery(final Expression<Boolean> expression, final WhereVisitor whereVisitor) {
		return buildQuery(new ThrowableQueryBuilder<E>() {
			@Override
			public QueryBuilder<E, Long> buildQuery(final QueryBuilder<E, Long> queryBuilder) throws SQLException {
				return AbstractDao.buildStatement(queryBuilder, expression, whereVisitor);
			}
		});
	}

	protected QueryBuilder<E, Long> buildQuery(final Expression<Boolean> expression, final WhereVisitor whereVisitor, final Order<E> order) {
		final Date now = getNow();
		return buildQuery(new ThrowableQueryBuilder<E>() {
			@Override
			public QueryBuilder<E, Long> buildQuery(final QueryBuilder<E, Long> queryBuilder) throws SQLException {
				return AbstractDao.buildQuery(queryBuilder, expression, whereVisitor, now, order);
			}
		});
	}

	@SuppressWarnings("unchecked")
	protected QueryBuilder<E, Long> buildQuery(final Expression<Boolean> expression, final WhereVisitor whereVisitor, final Order<E>... orders) {
		final Date now = getNow();
		return buildQuery(new ThrowableQueryBuilder<E>() {
			@Override
			public QueryBuilder<E, Long> buildQuery(final QueryBuilder<E, Long> queryBuilder) throws SQLException {
				return AbstractDao.buildQuery(queryBuilder, expression, whereVisitor, now, orders);
			}
		});
	}

	protected QueryBuilder<E, Long> buildQuery(final ThrowableQueryBuilder<E> throwableQueryBuilder) {
		final QueryBuilder<E, Long> queryBuilder = dao.queryBuilder();
		try {
			return throwableQueryBuilder.buildQuery(queryBuilder);
		} catch (final SQLException e) {
			LOGGER.info("buildQuery", e);
			return null;
		}
	}

	protected UpdateBuilder<E, Long> buildUpdate() {
		return dao.updateBuilder();
	}

	protected UpdateBuilder<E, Long> buildUpdate(final Expression<Boolean> expression) {
		return buildUpdate(new ThrowableUpdateBuilder<E>() {
			@Override
			public UpdateBuilder<E, Long> buildUpdate(final UpdateBuilder<E, Long> updateBuilder) throws SQLException {
				return AbstractDao.buildStatement(updateBuilder, expression, null);
			}
		});
	}

	protected UpdateBuilder<E, Long> buildUpdate(final Expression<Boolean> expression, final WhereVisitor whereVisitor) {
		return buildUpdate(new ThrowableUpdateBuilder<E>() {
			@Override
			public UpdateBuilder<E, Long> buildUpdate(final UpdateBuilder<E, Long> updateBuilder) throws SQLException {
				return AbstractDao.buildStatement(updateBuilder, expression, whereVisitor);
			}
		});
	}

	protected UpdateBuilder<E, Long> buildUpdate(final ThrowableUpdateBuilder<E> throwableUpdateBuilder) {
		final UpdateBuilder<E, Long> updateBuilder = dao.updateBuilder();
		try {
			return throwableUpdateBuilder.buildUpdate(updateBuilder);
		} catch (final SQLException e) {
			LOGGER.info("buildUpdate", e);
			return null;
		}
	}

	protected DeleteBuilder<E, Long> buildDelete() {
		return dao.deleteBuilder();
	}

	protected DeleteBuilder<E, Long> buildDelete(final Expression<Boolean> expression) {
		return buildDelete(new ThrowableDeleteBuilder<E>() {
			@Override
			public DeleteBuilder<E, Long> buildDelete(final DeleteBuilder<E, Long> deleteBuilder) throws SQLException {
				return AbstractDao.buildStatement(deleteBuilder, expression, null);
			}
		});
	}

	protected DeleteBuilder<E, Long> buildDelete(final Expression<Boolean> expression, final WhereVisitor whereVisitor) {
		return buildDelete(new ThrowableDeleteBuilder<E>() {
			@Override
			public DeleteBuilder<E, Long> buildDelete(final DeleteBuilder<E, Long> deleteBuilder) throws SQLException {
				return AbstractDao.buildStatement(deleteBuilder, expression, whereVisitor);
			}
		});
	}

	protected DeleteBuilder<E, Long> buildDelete(final ThrowableDeleteBuilder<E> throwableDeleteBuilder) {
		final DeleteBuilder<E, Long> deleteBuilder = dao.deleteBuilder();
		try {
			return throwableDeleteBuilder.buildDelete(deleteBuilder);
		} catch (final SQLException e) {
			LOGGER.info("buildDelete", e);
			return null;
		}
	}

	protected TableInfo<E, Long> getTableInfo() {
		return dao instanceof BaseDaoImpl ? ((BaseDaoImpl<E, Long>) dao).getTableInfo() : null;
	}

	protected Date getNow() {
		return database.getNow();
	}

	private static <E extends Entity> QueryBuilder<E, Long> buildQuery(final QueryBuilder<E, Long> queryBuilder, final Expression<Boolean> expression, final WhereVisitor whereVisitor, final Date now, final Order<E> order) {
		return orderByIfNotNull(buildStatement(queryBuilder, expression, whereVisitor), now, order);
	}

	@SafeVarargs
	private static <E extends Entity> QueryBuilder<E, Long> buildQuery(final QueryBuilder<E, Long> queryBuilder, final Expression<Boolean> expression, final WhereVisitor whereVisitor, final Date now, final Order<E>... orders) {
		return orderByIfNotNull(buildStatement(queryBuilder, expression, whereVisitor), now, orders);
	}

	private static <T extends StatementBuilder<?, ?>> T buildStatement(final T statementBuilder, final Expression<Boolean> expression, final WhereVisitor whereVisitor) {
		return whereVisitor != null ? whereIfNotNull(statementBuilder, expression, whereVisitor) : whereIfNotNull(statementBuilder, expression);
	}

	@SafeVarargs
	protected static <E extends Entity> QueryBuilder<E, Long> joinIfNotNull(final QueryBuilder<E, Long> queryBuilder, final QueryBuilder<? extends Entity, Long>... joinedQueryBuilders) throws SQLException {
		if ((queryBuilder == null) || (joinedQueryBuilders == null)) {
			return queryBuilder;
		}
		for (final QueryBuilder<? extends Entity, Long> joinedQueryBuilder : joinedQueryBuilders) {
			joinIfNotNull(queryBuilder, joinedQueryBuilder);
		}
		return queryBuilder;
	}

	protected static <E extends Entity> QueryBuilder<E, Long> joinIfNotNull(final QueryBuilder<E, Long> queryBuilder, final QueryBuilder<? extends Entity, Long> joinedQueryBuilder) throws SQLException {
		return (queryBuilder != null) && (joinedQueryBuilder != null) ? queryBuilder.join(joinedQueryBuilder) : queryBuilder;
	}

	@SafeVarargs
	protected static <E extends Entity> QueryBuilder<E, Long> leftJoinIfNotNull(final QueryBuilder<E, Long> queryBuilder, final QueryBuilder<? extends Entity, Long>... joinedQueryBuilders) throws SQLException {
		if ((queryBuilder == null) || (joinedQueryBuilders == null)) {
			return queryBuilder;
		}
		for (final QueryBuilder<? extends Entity, Long> joinedQueryBuilder : joinedQueryBuilders) {
			leftJoinIfNotNull(queryBuilder, joinedQueryBuilder);
		}
		return queryBuilder;
	}

	protected static <E extends Entity> QueryBuilder<E, Long> leftJoinIfNotNull(final QueryBuilder<E, Long> queryBuilder, final QueryBuilder<? extends Entity, Long> joinedQueryBuilder) throws SQLException {
		return (queryBuilder != null) && (joinedQueryBuilder != null) ? queryBuilder.leftJoin(joinedQueryBuilder) : queryBuilder;
	}

	@SafeVarargs
	protected static <E extends Entity> QueryBuilder<E, Long> orderByIfNotNull(final QueryBuilder<E, Long> queryBuilder, final Date now, final Order<E>... orders) {
		if ((queryBuilder == null) || (orders == null)) {
			return queryBuilder;
		}
		for (final Order<E> order : orders) {
			orderByIfNotNull(queryBuilder, now, order);
		}
		return queryBuilder;
	}

	protected static <E extends Entity> QueryBuilder<E, Long> orderByIfNotNull(final QueryBuilder<E, Long> queryBuilder, final Date now, final Order<E> order) {
		return (queryBuilder != null) && (order != null) ? order.orderBy(queryBuilder, now) : queryBuilder;
	}

	protected static <T extends StatementBuilder<?, ?>> T whereIfNotNull(final T statementBuilder, final Expression<Boolean> expression) {
		return whereIfNotNull(statementBuilder, expression, WHERE_VISITOR);
	}

	protected static <T extends StatementBuilder<?, ?>> T whereIfNotNull(final T statementBuilder, final Expression<Boolean> expression, final WhereVisitor whereVisitor) {
		if ((statementBuilder == null) || (expression == null) || (whereVisitor == null)) {
			return statementBuilder;
		}
		statementBuilder.where().raw(expression.accept(whereVisitor));
		return statementBuilder;
	}

	private static <E extends Entity> E find(final Dao<E, Long> dao, final long id) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find: " + ReflectionToStringBuilder.toString(dao) + ", " + id);
		}
		final E result = dao.queryForId(id);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find: " + result);
		}
		return result;
	}

	private static <E extends Entity> E find(final Dao<E, ?> dao, final QueryBuilder<E, ?> queryBuilder) throws SQLException {
		final PreparedQuery<E> preparedQuery = queryBuilder.prepare();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find: " + ReflectionToStringBuilder.toString(dao) + ", " + preparedQuery);
		}
		final E result = dao.queryForFirst(preparedQuery);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find: " + result);
		}
		return result;
	}

	private static <E extends Entity> List<E> findAll(final Dao<E, ?> dao) throws SQLException {
		final List<E> result = dao.queryForAll();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findAll: " + ReflectionToStringBuilder.toString(dao));
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findAll: " + result);
		}
		return result;
	}

	private static <E extends Entity> List<E> findAll(final Dao<E, ?> dao, final QueryBuilder<E, ?> queryBuilder) throws SQLException {
		final PreparedQuery<E> preparedQuery = queryBuilder.prepare();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findAll: " + ReflectionToStringBuilder.toString(dao) + ", " + preparedQuery);
		}
		final List<E> result = dao.query(preparedQuery);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("findAll: " + result);
		}
		return result;
	}

	private static <E extends Entity> boolean insert(final Dao<E, ?> dao, final E entity, final Date now) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insert: " + ReflectionToStringBuilder.toString(dao) + ", " + entity);
		}
		entity.setCreatedAt(now);
		final boolean result = dao.create(entity) == 1;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insert: " + result + ", " + entity);
		}
		return result;
	}

	private static <E extends Entity> boolean insertOrUpdate(final Dao<E, ?> dao, final E entity, final Date now) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insertOrUpdate: " + ReflectionToStringBuilder.toString(dao) + ", " + entity);
		}
		final Date createdAt = entity.getCreatedAt();
		entity.setCreatedAt(createdAt != null ? createdAt : now);
		final Date updatedAt = entity.getUpdatedAt();
		entity.setUpdatedAt(updatedAt != null ? updatedAt : now);
		final CreateOrUpdateStatus status = dao.createOrUpdate(entity);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insertOrUpdate: " + ReflectionToStringBuilder.toString(status) + ", " + entity);
		}
		final boolean created = (status != null) && status.isCreated();
		final boolean updated = (status != null) && status.isUpdated();
		entity.setCreatedAt(created ? now : createdAt);
		entity.setUpdatedAt(updated ? now : updatedAt);
		final boolean result = (updated && ((updatedAt == null) || now.equals(updatedAt))) || (dao.update(entity) == 1);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("insertOrUpdate: " + result + ", " + entity);
		}
		return result;
	}

	private static <E extends Entity> boolean update(final Dao<E, ?> dao, final E entity, final Date now) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("update: " + ReflectionToStringBuilder.toString(dao) + ", " + entity);
		}
		entity.setUpdatedAt(now);
		final boolean result = dao.update(entity) == 1;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("update: " + result + ", " + entity);
		}
		return result;
	}

	private static <E extends Entity> long updateAll(final Dao<E, ?> dao, final UpdateBuilder<E, ?> updateBuilder, final Date now) throws SQLException {
		updateBuilder.updateColumnValue(Entity.COLUMN_NAME_UPDATED_AT, now);
		final PreparedUpdate<E> preparedUpdate = updateBuilder.prepare();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("updateAll: " + ReflectionToStringBuilder.toString(dao) + ", " + preparedUpdate);
		}
		final long result = dao.update(preparedUpdate);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("updateAll: " + result);
		}
		return result;
	}

	private static <E extends Entity> boolean delete(final Dao<E, ?> dao, final E entity) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("delete: " + ReflectionToStringBuilder.toString(dao) + ", " + entity);
		}
		final boolean result = dao.delete(entity) == 1;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("delete: " + result);
		}
		return result;
	}

	private static <E extends Entity> long deleteAll(final Dao<E, ?> dao) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("deleteAll: " + ReflectionToStringBuilder.toString(dao));
		}
		final long result = deleteAll(dao, dao.deleteBuilder());
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("deleteAll: " + result);
		}
		return result;
	}

	private static <E extends Entity> long deleteAll(final Dao<E, ?> dao, final DeleteBuilder<E, ?> deleteBuilder) throws SQLException {
		final PreparedDelete<E> preparedDelete = deleteBuilder.prepare();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("deleteAll: " + ReflectionToStringBuilder.toString(dao) + ", " + preparedDelete);
		}
		final long result = dao.delete(preparedDelete);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("deleteAll: " + result);
		}
		return result;
	}

	private static <E extends Entity> long count(final Dao<E, ?> dao) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("count: " + ReflectionToStringBuilder.toString(dao));
		}
		final long result = dao.countOf();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("count: " + result);
		}
		return result;
	}

	private static <E extends Entity> long count(final Dao<E, ?> dao, final QueryBuilder<E, ?> queryBuilder) throws SQLException {
		final PreparedQuery<E> preparedQuery = queryBuilder.setCountOf(true).prepare();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("count: " + ReflectionToStringBuilder.toString(dao) + ", " + preparedQuery);
		}
		final long result = dao.countOf(preparedQuery);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("count: " + result);
		}
		return result;
	}

	private static DatabaseConnection begin(final Dao<?, ?> dao) throws SQLException {
		final DatabaseConnection connection = dao.startThreadConnection();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("begin: " + ReflectionToStringBuilder.toString(dao) + ", " + connection);
		}
		dao.setAutoCommit(connection, false);
		return connection;
	}

	private static void commit(final Dao<?, ?> dao, final DatabaseConnection connection) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("commit: " + ReflectionToStringBuilder.toString(dao) + ", " + connection);
		}
		dao.commit(connection);
		dao.endThreadConnection(connection);
	}

	private static void rollback(final Dao<?, ?> dao, final DatabaseConnection connection) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("rollback: " + ReflectionToStringBuilder.toString(dao) + ", " + connection);
		}
		dao.rollBack(connection);
		dao.endThreadConnection(connection);
	}
}
