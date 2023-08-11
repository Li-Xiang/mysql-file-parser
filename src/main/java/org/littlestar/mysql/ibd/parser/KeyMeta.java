package org.littlestar.mysql.ibd.parser;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KeyMeta {
	private long indexId;
	private final Map<Integer, ColumnMeta> keyColumnMetaMap;

	public KeyMeta() {
		keyColumnMetaMap = new HashMap<Integer, ColumnMeta>();
	}

	public long getIndexId() {
		return indexId;
	}

	public KeyMeta setIndexId(long indexId) {
		this.indexId = indexId;
		return this;
	}

	public Map<Integer, ColumnMeta> getKeyColumnMetaMap() {
		return keyColumnMetaMap;
	}

	/**
	 * 
	 * @param seqInindex Seq_in_index
	 * @param column column meta
	 * @return
	 */
	public KeyMeta setKeyColumn(int seqInindex, ColumnMeta column) {
		keyColumnMetaMap.put(seqInindex, column);
		return this;
	}

	public List<ColumnMeta> getKeyColumns() {
		Collection<ColumnMeta> columnMetas = keyColumnMetaMap.values();
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
}
