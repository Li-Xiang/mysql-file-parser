package org.littlestar.mysql.ibd.page;

import static org.littlestar.mysql.common.ParserHelper.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import org.littlestar.mysql.ibd.parser.ColumnMeta;
import org.littlestar.mysql.ibd.parser.KeyMeta;

public class SecondaryKeyLeafPage extends IndexPage {

	public SecondaryKeyLeafPage(byte[] pageRaw, int pageSize) {
		super(pageRaw, pageSize);
	}

	public SecondaryKeyLeafPage(byte[] pageRaw) {
		super(pageRaw);
	}
	
	private List<SecondaryKeyLeafRecord> iterateRecordInPage(KeyMeta secondaryKeyMeta, KeyMeta clusterKeyMeta, int firstRecordPos) {
		final List<SecondaryKeyLeafRecord> secondaryKeyLeafRecords = new ArrayList<SecondaryKeyLeafRecord>();
		int currentPos = firstRecordPos;
		int recCount = 0;
		while (currentPos > SUPREMUM_EXTRA_END_POS && currentPos <= getIndexHeader().getHeapTopPosition()) {
			final List<RecordField> secondaryKeyFields = new ArrayList<RecordField>();
			final List<RecordField> clusterKeyFields = new ArrayList<RecordField>();
			int nullableColumnCount = 0;
			for (ColumnMeta meta : secondaryKeyMeta.getKeyColumns()) {
				secondaryKeyFields.add(new RecordField(meta));
				if (meta.isNullable()) {
					nullableColumnCount++;
				}
			}
			for (ColumnMeta meta : clusterKeyMeta.getKeyColumns()) {
				clusterKeyFields.add(new RecordField(meta));
				if (meta.isNullable()) {
					nullableColumnCount++;
				}
			}
			
			final List<RecordField> combinKeyFields = new ArrayList<RecordField>();
			combinKeyFields.addAll(secondaryKeyFields); 
			combinKeyFields.addAll(clusterKeyFields);
			
			SecondaryKeyLeafRecord userRecord = new SecondaryKeyLeafRecord();
			int from = currentPos - REC_N_NEW_EXTRA_BYTES;
			int to = currentPos;
			byte[] recordExtraRaw = Arrays.copyOfRange(pageRaw, from, to);
			userRecord.setRecordExtraRaw(recordExtraRaw);
			int nextOffset = userRecord.getNextRecordOffset();
			int nextRecord = nextOffset + to;
			
			final int nullableBitmapBytes = (nullableColumnCount + 7) / 8;
			final BitSet nullBitmap;
			if (nullableBitmapBytes > 0) {
				to = from;
				from -= nullableBitmapBytes;
				byte[] nullBitmapRaw = Arrays.copyOfRange(pageRaw, from, to);
				nullBitmap = toBitSet(nullBitmapRaw);
				int bitmapIndex = 0;
				// set null field by null-bitmap.
				for (RecordField field : combinKeyFields) {
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
			for (ColumnMeta field : combinKeyFields) {
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
			
			secondaryKeyLeafRecords.add(userRecord);
			recCount++;
			if (recCount > maxRecs || nextOffset == 0 || nextRecord == SUPREMUM_EXTRA_END_POS) {
				break;
			}
			currentPos = nextRecord;
		}
		return secondaryKeyLeafRecords;
	}
	
	public List<SecondaryKeyLeafRecord> getUserRecords(KeyMeta secondaryKeyMeta, KeyMeta clusterKeyMeta) {
		if (Objects.isNull(clusterKeyMeta) || Objects.isNull(secondaryKeyMeta)) {
			throw new IllegalArgumentException("CluserKey or SecondaryKey meta in TableMeta is null.");
		}
		int pos = getSystemRecords().getInfimumNextRecordPos();
		return iterateRecordInPage(secondaryKeyMeta, clusterKeyMeta, pos);
	}
	
	/**
	 * Record Format - Secondary Key - Leaf Pages:
	 * -------------------------------------------
	 * (Variable Length Record Header)
	 * Secondary Key Fields (k)
	 * Cluster Key Fields (j)
	 */
	public class SecondaryKeyLeafRecord extends RecordExtra {
		private final List<RecordField> secondaryKeyFields;
		private final List<RecordField> clusterKeyFields;

		public SecondaryKeyLeafRecord() {
			secondaryKeyFields = new ArrayList<RecordField>();
			clusterKeyFields = new ArrayList<RecordField>();
		}

		/**
		 * Add Secondary Key(Index) Field (record). 注意: 索引/主键中如果包含多个字段, 这些字段是有先后顺序的,
		 * 添加字段内容时候也应该按索引/主键中的顺序添加。
		 * 
		 * @param recordField
		 * @return
		 */
		public SecondaryKeyLeafRecord addSecondaryKeyField(RecordField recordField) {
			secondaryKeyFields.add(recordField);
			return this;
		}

		public List<RecordField> getSecondaryKeyFields() {
			return secondaryKeyFields;
		}

		/**
		 * Add Cluster Key(PK) Field (record). 注意: 索引/主键中如果包含多个字段, 这些字段是有先后顺序的,
		 * 添加字段内容时候也应该按索引/主键中的顺序添加。
		 * 
		 * @param recordField
		 * @return
		 */
		public SecondaryKeyLeafRecord addClusterKeyField(RecordField recordField) {
			clusterKeyFields.add(recordField);
			return this;
		}

		public List<RecordField> getClusterKeyFields() {
			return clusterKeyFields;
		}
	}
}
