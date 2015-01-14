package com.github.t3t5u.common.ormlite;

import java.util.Date;

import javax.persistence.Column;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.github.t3t5u.common.database.Entity;

@SuppressWarnings("serial")
public abstract class AbstractJoinableEntity implements Entity {
	@Column(name = COLUMN_NAME_CREATED_AT, nullable = false)
	private Date createdAt;
	@Column(name = COLUMN_NAME_UPDATED_AT)
	private Date updatedAt;

	@Override
	public Date getCreatedAt() {
		return createdAt;
	}

	@Override
	public void setCreatedAt(final Date createdAt) {
		this.createdAt = createdAt;
	}

	@Override
	public Date getUpdatedAt() {
		return updatedAt;
	}

	@Override
	public void setUpdatedAt(final Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
