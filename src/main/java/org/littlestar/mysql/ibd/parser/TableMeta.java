package org.littlestar.mysql.ibd.parser;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TableMeta {
	private String tableName;
	private String rowFormat;

	private final HashMap<Integer, ColumnMeta> columnMap;
	private KeyMeta clusterKey;
	private final HashMap<Long, KeyMeta> secondaryKeys;

	public TableMeta() {
		columnMap = new HashMap<Integer, ColumnMeta>();
		clusterKey = new KeyMeta();
		secondaryKeys = new HashMap<Long, KeyMeta>();
	}

	public ColumnMeta getColumn(int pos) {
		return columnMap.get(pos);
	}
	
	public ColumnMeta getColumn(String columnName) {
		for (ColumnMeta column : columnMap.values()) {
			if (Objects.equals(column.getName(), columnName)) {
				return column;
			}
		}
		return null;
	}

	public TableMeta addColumn(ColumnMeta column) {
		if (Objects.nonNull(column)) {
			Integer pos = column.getPos();
			if (Objects.nonNull(pos)) {
				columnMap.put(pos, column);
			}
		}
		return this;
	}

	public Map<Integer, ColumnMeta> getColumnMap() {
		return columnMap;
	}

	public TableMeta setClusterKey(KeyMeta keyMeta) {
		clusterKey = keyMeta;
		return this;
	}

	public TableMeta setClusterKeyId(long indexId) {
		clusterKey.setIndexId(indexId);
		return this;
	}
	
	public TableMeta setClusterKey(int seqInindex, ColumnMeta column) {
		clusterKey.setKeyColumn(seqInindex, column);
		return this;
	}

	public KeyMeta getClusterKey() {
		return clusterKey;
	}

	public KeyMeta getSecondaryKey(long indexId) {
		return secondaryKeys.get(indexId);
	}

	public Map<Long, KeyMeta> getSecondaryKeys() {
		return secondaryKeys;
	}

	public TableMeta setSecondaryKey(KeyMeta keyMeta) {
		secondaryKeys.put(keyMeta.getIndexId(), keyMeta);
		return this;
	}
	
	public TableMeta setSecondaryKey(long indexId, int seqInindex, ColumnMeta column) {
		if (Objects.isNull(column)) {
			throw new RuntimeException("column meta of secondary key is null.");
		}
		KeyMeta key = secondaryKeys.get(indexId);
		if (Objects.isNull(key)) {
			key = new KeyMeta();
			key.setIndexId(indexId);
			secondaryKeys.put(indexId, key);
		}
		key.setKeyColumn(seqInindex, column);
		return this;
	}
	
	public TableMeta setSecondaryKey(long indexId, int seqInindex, int colPosInTable) {
		ColumnMeta column = getColumn(colPosInTable);
		if (Objects.isNull(column)) {
			throw new RuntimeException(
					"column meta not found or is null in table meta: colPosInTable = " + colPosInTable);
		}
		return setSecondaryKey(indexId, seqInindex, column);
	}
	
	public TableMeta setSecondaryKey(long indexId, int seqInindex, String columnName) {
		ColumnMeta column = getColumn(columnName);
		if (Objects.isNull(column)) {
			throw new RuntimeException("column meta not found or is null in table meta: columnName = " + columnName);
		}
		return setSecondaryKey(indexId, seqInindex, column);
	}

	/**
	 * 获取表的字段列表，按字段位置(pos)升序排序。
	 * 
	 * @return 获取有序的表的字段列表
	 */
	public List<ColumnMeta> getColumns() {
		Collection<ColumnMeta> columnMetas = getColumnMap().values();
		Comparator<ColumnMeta> comparator = new Comparator<ColumnMeta>() {
			@Override
			public int compare(ColumnMeta col1, ColumnMeta col2) {
				Integer col1Pos = col1.getPos();
				Integer col2Pos = col2.getPos();
				int comp = col1Pos.compareTo(col2Pos);
				return comp;
			}
		};
		return columnMetas.stream().sorted(comparator).collect(Collectors.toList());
	}
	
	public int getColumnCount() {
		return columnMap.size();
	}
	
	public int getNullableColumnCount() {
		Collection<ColumnMeta> metas = getColumnMap().values();
		int count = 0;
		for (ColumnMeta meta : metas) {
			if (meta.isNullable()) {
				count++;
			}
		}
		return count;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getRowFormat() {
		return rowFormat;
	}

	public void setRowFormat(String rowFormat) {
		this.rowFormat = rowFormat;
	}

}
