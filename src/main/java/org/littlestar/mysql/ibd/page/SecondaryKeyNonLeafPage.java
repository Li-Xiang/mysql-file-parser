package org.littlestar.mysql.ibd.page;

import static org.littlestar.mysql.common.ParserHelper.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import org.littlestar.mysql.ibd.parser.ColumnMeta;
import org.littlestar.mysql.ibd.parser.KeyMeta;
import org.littlestar.mysql.ibd.parser.TableMeta;

public class SecondaryKeyNonLeafPage extends IndexPage {

	public SecondaryKeyNonLeafPage(byte[] pageRaw, int pageSize) {
		super(pageRaw, pageSize);
	}

	public SecondaryKeyNonLeafPage(byte[] pageRaw) {
		super(pageRaw);
	}

	/**
	 * TableMeta必须包含当前叶的indexId的定义(KeyMeta).
	 * 
	 * @see #getUserRecords(TableMeta, long)
	 * @param tableMeta
	 * @return record list of this page
	 */
	public List<SecondaryKeyNonLeafRecord> getUserRecords(TableMeta tableMeta) {
		long indexId = getIndexHeader().getIndexId().longValueExact();
		return getUserRecords(tableMeta, indexId);
	}
	
	/**
	 * TableMeta必须包含自定的indexId的定义(KeyMeta).
	 * 
	 * @see #getUserRecords(TableMeta, KeyMeta)
	 * @param tableMeta
	 * @return record list of this page
	 */
	public List<SecondaryKeyNonLeafRecord> getUserRecords(TableMeta tableMeta, long indexId) {
		KeyMeta secondaryKeyMeta = tableMeta.getSecondaryKey(indexId);
		KeyMeta clusterKeyMeta = tableMeta.getClusterKey();
		if (Objects.isNull(secondaryKeyMeta)) {
			throw new RuntimeException("secondary key meta not found or is null in table meta, index id: " + indexId);
		}
		if (Objects.isNull(clusterKeyMeta)) {
			throw new RuntimeException("cluster key meta not found or is null in table meta");
		}
		int pos = getSystemRecords().getInfimumNextRecordPos();
		return iterateRecordInPage(secondaryKeyMeta, clusterKeyMeta, pos);
	}

	/**
	 * 根据给定的表定义和索引定义, 解析当前页的记录。
	 * 
	 * @param tableMeta        表定义
	 * @param secondaryKeyMeta 索引定义
	 * @return record list of this page
	 */
	public List<SecondaryKeyNonLeafRecord> getUserRecords(KeyMeta secondaryKeyMeta, KeyMeta clusterKeyMeta) {
		int pos = getSystemRecords().getInfimumNextRecordPos();
		return iterateRecordInPage(secondaryKeyMeta, clusterKeyMeta, pos);
	}
	
	private List<SecondaryKeyNonLeafRecord> iterateRecordInPage(KeyMeta secondaryKeyMeta, KeyMeta clusterKeyMeta, int firstRecordPos) {
		final List<SecondaryKeyNonLeafRecord> records = new ArrayList<SecondaryKeyNonLeafRecord>();
		int currentPos = firstRecordPos;
		int recCount = 0;
		while (currentPos > SUPREMUM_EXTRA_END_POS && currentPos <= getIndexHeader().getHeapTopPosition()) {
			final List<RecordField> secondaryKeyFields = new ArrayList<RecordField>();
			final List<RecordField> clusterKeyFields = new ArrayList<RecordField>();
			int nullableBitmapBytes = 0;
			for (ColumnMeta meta : secondaryKeyMeta.getKeyColumns()) {
				if (meta.isNullable()) {
					nullableBitmapBytes++;
				}
				secondaryKeyFields.add(new RecordField(meta));
			}
			for (ColumnMeta meta : clusterKeyMeta.getKeyColumns()) {
				clusterKeyFields.add(new RecordField(meta));
			}
			final List<RecordField> contentFields = new ArrayList<RecordField>();
			contentFields.addAll(secondaryKeyFields); // Secondary Key first.
			contentFields.addAll(clusterKeyFields);
			
			SecondaryKeyNonLeafRecord userRecord = new SecondaryKeyNonLeafRecord();
			int from = currentPos - REC_N_NEW_EXTRA_BYTES;
			int to = currentPos;
			byte[] recordExtraRaw = Arrays.copyOfRange(pageRaw, from, to);
			userRecord.setRecordExtraRaw(recordExtraRaw);
			int nextOffset = userRecord.getNextRecordOffset();
			int nextRecord = nextOffset + to;
			
			final BitSet nullBitmap;
			if (nullableBitmapBytes > 0) {
				to = from;
				from -= nullableBitmapBytes;
				byte[] nullBitmapRaw = Arrays.copyOfRange(pageRaw, from, to);
				nullBitmap = toBitSet(nullBitmapRaw);
				int bitmapIndex = 0;
				for (RecordField field : contentFields) {
					if (field.isNullable()) {
						boolean isNull = nullBitmap.get(bitmapIndex++);
						field.setNull(isNull);
						if (isNull) {
							field.setLength(0);
						}
					}
				}
			} else {
				nullBitmap = new BitSet();
			}

			int vfrom = 0, vto = from;
			for (ColumnMeta field : contentFields) {
				if (field.isVariableLength()) {
					int len = field.getLength();
					if (len > 0xFF) {
						byte[] bytes = Arrays.copyOfRange(pageRaw, vfrom = vto - 2, vto);
						int b1 = getUInt8(bytes[1]);
						if ((b1 & 0x80) == 0) {
							byte b = Arrays.copyOfRange(pageRaw, vfrom = vto - 1, vto)[0];
							len = getUInt8(b);
						} else {
							int b0 = getUInt8(bytes[0]);
							b1 <<= 8;
							b1 |= b0;
							int offs = 0;
							offs += b1 & 0x3fff;
							if ((b1 & 0x4000) != 0) {
								int REC_OFFS_EXTERNAL = 1 << 30;
								len = offs | REC_OFFS_EXTERNAL;
							} else {
								len = offs;
							}
						}
					} else {
						byte b = Arrays.copyOfRange(pageRaw, vfrom = vto - 1, vto)[0];
						len = getUInt8(b);
					}
					field.setLength(len);
					vto = vfrom;
				}
			}

			int cententOffset = currentPos;
			for (RecordField field : secondaryKeyFields) {
				byte[] contentRaw = null;
				if (!field.isNull()) {
					contentRaw = Arrays.copyOfRange(pageRaw, cententOffset, cententOffset += field.getLength());
				}
				field.setConetentRaw(contentRaw);
				userRecord.addSecondaryKeyField(field);
			}
			for (RecordField field : clusterKeyFields) {
				byte[] contentRaw = Arrays.copyOfRange(pageRaw, cententOffset, cententOffset += field.getLength());
				field.setConetentRaw(contentRaw);
				userRecord.addClusterKeyField(field);
			}
			byte[] childPageNoRaw = Arrays.copyOfRange(pageRaw, cententOffset, cententOffset += 4);
			long childPageNo = getUInt32(childPageNoRaw);
			userRecord.setChildPageNumber(childPageNo);

			records.add(userRecord);
			recCount++;
			if (recCount > maxRecs || nextOffset == 0 || nextRecord == SUPREMUM_EXTRA_END_POS) {
				break;
			}
			currentPos = nextRecord;
		}
		return records;
	}

	public class SecondaryKeyNonLeafRecord extends RecordExtra {
		private List<RecordField> secondaryKeyFields;
		private List<RecordField> clusterKeyFields;
		private long childPageNumber;

		public SecondaryKeyNonLeafRecord() {
			secondaryKeyFields = new ArrayList<RecordField>();
			clusterKeyFields = new ArrayList<RecordField>();
		}

		public long getChildPageNumber() {
			return childPageNumber;
		}

		public void setChildPageNumber(long childPageNumber) {
			this.childPageNumber = childPageNumber;
		}

		/**
		 * Add Secondary Key(Index) Field (record). 注意: 索引/主键中如果包含多个字段, 这些字段是有先后顺序的,
		 * 添加字段内容时候也应该按索引/主键中的顺序添加。
		 * 
		 * @param recordField
		 * @return
		 */
		public SecondaryKeyNonLeafRecord addSecondaryKeyField(RecordField recordField) {
			secondaryKeyFields.add(recordField);
			return this;
		}

		/**
		 * Secondary Key Min. Key on Child Page.
		 * 
		 * @return
		 */
		public List<RecordField> getMinSecondaryKeyOnChild() {
			return secondaryKeyFields;
		}

		/**
		 * Add Cluster Key(PK) Field (record). 注意: 索引/主键中如果包含多个字段, 这些字段是有先后顺序的,
		 * 添加字段内容时候也应该按索引/主键中的顺序添加。
		 * 
		 * @param recordField
		 * @return
		 */
		public SecondaryKeyNonLeafRecord addClusterKeyField(RecordField recordField) {
			clusterKeyFields.add(recordField);
			return this;
		}

		/**
		 * Cluster Key Min. Key on Child Page.
		 * 
		 * @return
		 */
		public List<RecordField> getMinClusterKeyOnChild() {
			return clusterKeyFields;
		}
	}
}