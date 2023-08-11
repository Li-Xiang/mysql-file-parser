package org.littlestar.mysql.ibd.page;

import static org.littlestar.mysql.common.ParserHelper.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.littlestar.mysql.ibd.parser.ColumnMeta;
import org.littlestar.mysql.ibd.parser.TableMeta;

public class ClusteredKeyNonLeafPage extends IndexPage {

	public ClusteredKeyNonLeafPage(byte[] pageRaw, int pageSize) {
		super(pageRaw, pageSize);
	}

	public ClusteredKeyNonLeafPage(byte[] pageRaw) {
		super(pageRaw);
	}

	private List<ClusteredKeyNonLeafRecord> iterateRecordInPage(TableMeta tableMeta, int firstRecordPos) {
		final List<ClusteredKeyNonLeafRecord> clusteredKeyNonLeafRecords = new ArrayList<ClusteredKeyNonLeafRecord>();
		int currentPos = firstRecordPos;
		int recCount = 0;
		while (currentPos > SUPREMUM_EXTRA_END_POS && currentPos <= getIndexHeader().getHeapTopPosition()) {
			final List<RecordField> clusterKeyFields = new ArrayList<RecordField>();
			for (ColumnMeta meta : tableMeta.getClusterKey().getKeyColumns()) {
				clusterKeyFields.add(new RecordField(meta));
			}
			
			ClusteredKeyNonLeafRecord userRecord = new ClusteredKeyNonLeafRecord();
			int from = currentPos - REC_N_NEW_EXTRA_BYTES;
			int to = currentPos;
			byte[] recordExtraRaw = Arrays.copyOfRange(pageRaw, from, to);
			userRecord.setRecordExtraRaw(recordExtraRaw);
			int nextOffset = userRecord.getNextRecordOffset();
			int nextRecord = nextOffset + to;

			// 主键的非叶点节记录会存储整个表的空值位图, MySQL主键的所有字段都不允许为空, 不理解为什么要存储null-field-bitmap。
			// ERROR 1171 (42000): All parts of a PRIMARY KEY must be NOT NULL; if you need
			// NULL in a key, use UNIQUE instead
			int nullableBitmapBytes = (tableMeta.getNullableColumnCount() + 7) / 8;
			from -= nullableBitmapBytes;

			int vfrom = 0, vto = from;
			for (ColumnMeta field : clusterKeyFields) {
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
			for (RecordField field : clusterKeyFields) {
				byte[] contentRaw = Arrays.copyOfRange(pageRaw, cententOffset, cententOffset += field.getLength());
				field.setConetentRaw(contentRaw);
				userRecord.addMinClusterKeyField(field);
			}
			byte[] childPageNoRaw = Arrays.copyOfRange(pageRaw, cententOffset, cententOffset += 4);
			long childPageNo = getUInt32(childPageNoRaw);
			userRecord.setChildPageNumber(childPageNo);
			//
			clusteredKeyNonLeafRecords.add(userRecord);
			recCount++;
			if (recCount > maxRecs || nextOffset == 0 || nextRecord == SUPREMUM_EXTRA_END_POS) {
				break;
			}
			currentPos = nextRecord;
		}
		return clusteredKeyNonLeafRecords;
	}

	public List<ClusteredKeyNonLeafRecord> getUserRecords(TableMeta tableMeta) {
		int pos = getSystemRecords().getInfimumNextRecordPos();
		return iterateRecordInPage(tableMeta, pos);
	}

	public class ClusteredKeyNonLeafRecord extends RecordExtra {
		private final List<RecordField> clusterKeyFields;
		private long childPageNumber;

		public ClusteredKeyNonLeafRecord() {
			clusterKeyFields = new ArrayList<RecordField>();
		}

		public long getChildPageNumber() {
			return childPageNumber;
		}

		public void setChildPageNumber(long childPageNumber) {
			this.childPageNumber = childPageNumber;
		}

		/**
		 * Add Clustered Key(PK) Field (record). 注意: 索引/主键中如果包含多个字段, 这些字段是有先后顺序的,
		 * 添加字段内容时候也应该按索引/主键中的顺序添加。
		 * 
		 * @param recordField
		 * @return
		 */
		public ClusteredKeyNonLeafRecord addMinClusterKeyField(RecordField recordField) {
			clusterKeyFields.add(recordField);
			return this;
		}

		public List<RecordField> getMinClusterKeyOnChild() {
			return clusterKeyFields;
		}
	}
	
}