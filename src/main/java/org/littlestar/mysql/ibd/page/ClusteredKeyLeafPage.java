package org.littlestar.mysql.ibd.page;

import static org.littlestar.mysql.common.ParserHelper.getUInt8;
import static org.littlestar.mysql.common.ParserHelper.toBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import org.littlestar.mysql.ibd.parser.ColumnMeta;
import org.littlestar.mysql.ibd.parser.TableMeta;

public class ClusteredKeyLeafPage extends IndexPage {

	public ClusteredKeyLeafPage(byte[] pageRaw, int pageSize) {
		super(pageRaw, pageSize);
	}

	public ClusteredKeyLeafPage(byte[] pageRaw) {
		super(pageRaw);
	}
	
	/**
	 *  
	 * Parse user record by infimum's next record offset. the infimum's next record offset point to 
	 * the position of 1st user record's contents.
	 * 
	 * Note: this page must be Clustered Key(Primary Key) - Leaf Pages (level=0)
	 * 
	 * <pre>
	 * Record Format - Clustered Key - Leaf Pages:
	 * +---------------------------------+
	 * | (Variable Length Record Header) |
	 * +---------------------------------+
	 * |       Cluster Key Fields (k)    |
	 * +---------------------------------+
	 * |         Transaction ID (6)      |
	 * +---------------------------------+
	 * |          Roll Pointer (7)       |
	 * +---------------------------------+
	 * |         Non-Key Fields (j)      |
	 * +---------------------------------+
	 * 
	 * <pre>
	 * 
	 * <pre>
	 *  
	 *                                 |      First User Record          |                               
	 *                                 |   User Record Header (Variable) |  
	 * infimum.next_record_offset -->  +---------------------------------+
	 *                                 | User record Contents (Variable) |
	 * </pre>
	 * 
	 * @param tableMeta
	 * @param firstRecordPos first record position (infimum next record position)
	 * @return user records in this page.
	 */
	private List<ClusteredKeyLeafRecord> iterateRecordInPage(TableMeta tableMeta, int firstRecordPos) {
		final List<ClusteredKeyLeafRecord> records = new ArrayList<ClusteredKeyLeafRecord>();
		// the position of first user record's content in pageRaw.
		int currentPos = firstRecordPos;
		int recCount = 0;
		while (currentPos > SUPREMUM_EXTRA_END_POS && currentPos <= getIndexHeader().getHeapTopPosition()) {
			final List<RecordField> recordFields = new ArrayList<RecordField>();
			//// initial user record's filed list and get nullable field count.
			int nullableColumnCount = 0;
			for (ColumnMeta meta : tableMeta.getColumns()) {
				if (meta.isNullable()) {
					nullableColumnCount++;
				}
				recordFields.add(new RecordField(meta));
			}

			ClusteredKeyLeafRecord userRecord = new ClusteredKeyLeafRecord();
			////// record extra. //////
			int from = currentPos - REC_N_NEW_EXTRA_BYTES;
			int to = currentPos;
			// get record-extra.
			byte[] recordExtraRaw = Arrays.copyOfRange(pageRaw, from, to);
			userRecord.setRecordExtraRaw(recordExtraRaw);
			int nextOffset = userRecord.getNextRecordOffset();
			int nextRecord = nextOffset + to;

			////// null-bitmap. //////
			//// if not nullable column in table, then user record not contains null-bitmap.
			final int nullableBitmapBytes = (nullableColumnCount + 7) / 8;
			final BitSet nullBitmap;
			if (nullableBitmapBytes > 0) {
				to = from;
				from -= nullableBitmapBytes;
				byte[] nullBitmapRaw = Arrays.copyOfRange(pageRaw, from, to);
				userRecord.setNullBitmapRaw(nullBitmapRaw);
				nullBitmap = toBitSet(nullBitmapRaw);
				int bitmapIndex = 0;
				// set null field by null-bitmap.
				for (RecordField field : recordFields) {
					if (field.isNullable()) {
						boolean isNull = nullBitmap.get(bitmapIndex++);
						field.setNull(isNull);
						if (isNull) {
							field.setLength(0);
						}
					}
				}
			} else {
				userRecord.setNullBitmapRaw(null);
				nullBitmap = new BitSet();
			}

			////// variable-lengths. //////
			// get variable-length fields store length(bytes).
			int vfrom = 0, vto = from;
			for (RecordField field : recordFields) {
				if (field.isVariableLength() && (!field.isNull())) {
					int len = field.getLength();
					if (len > 0xFF) {
						// 参考undrop-for-innodb的c_parser.c的ibrec_init_offsets_new
						// 但没读懂意图, C++体育老师教的, 强改成了Java, 可能会有问题...
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
						//
					} else {
						byte b = Arrays.copyOfRange(pageRaw, vfrom = vto - 1, vto)[0];
						len = getUInt8(b);
					}
					field.setLength(len);
					vto = vfrom;
				}
			}

			byte[] variableFieldLengths;
			if (vfrom == 0 || vfrom >= to) {
				variableFieldLengths = null;
			} else {
				variableFieldLengths = Arrays.copyOfRange(pageRaw, vfrom, to);
			}
			userRecord.setVariableFieldLengthsRaw(variableFieldLengths);

			////// User Record Content.//////
			int cententOffset = currentPos;
			for (RecordField field : recordFields) {
				byte[] contentRaw = null;
				if (!field.isNull()) {
					contentRaw = Arrays.copyOfRange(pageRaw, cententOffset, cententOffset += field.getLength());
				}
				field.setConetentRaw(contentRaw);
				userRecord.addRecordField(field);
			}
			records.add(userRecord);
			recCount++;
			if (recCount > maxRecs || nextOffset == 0 || nextRecord == SUPREMUM_EXTRA_END_POS) {
				break;
			}
			//// set next user record position
			currentPos = nextRecord;
		}
		return records;
	}
	
	/**
	 * get the user records in page.
	 * 
	 * @param tableMeta the table meta data.
	 * @return the user records in page.
	 */
	public List<ClusteredKeyLeafRecord> getUserRecords(TableMeta tableMeta) {
		int pos = getSystemRecords().getInfimumNextRecordPos();
		return iterateRecordInPage(tableMeta, pos);
	}

	/**
	 * get the garbage/deleted user records in page.
	 * 
	 * @param tableMeta the table meta data.
	 * 
	 * @return the garbage/deleted records in page
	 */
	public List<ClusteredKeyLeafRecord> getGarbageRecords(TableMeta tableMeta) {
		int pos = getIndexHeader().getFirstGarbageOffset();
		return iterateRecordInPage(tableMeta, pos);
	}
	
	/**
	 * The User Record.
	 * 
	 * <pre>
	 * +---------------------------------------------------------------------+
	 * |                               User Record                           |
	 * +------------------------+-------------+------------------------------+
	 * | variable-field-lengths | null-bitmap | user-record-extra | contents |
	 * +------------------------+-------------+------------------------------+
	 * 
	 * variable-field-lengths: (variable), the list of variable-length filed 's store bytes in contents.
	 *            null-bitmap: (variable), the bitmap of non-pk and non-internal columns. 1 if null.
	 *      user-record-extra: (fixed), 5 bytes for compact/dynamic row format.
	 *               contents: (variable), field contents.
	 * </pre>
	 *
	 *
	 * <pre>
	 * Contents (Clustered Key - Leaf Page)
	 * -----------------------+--------------------+------------------+--------------------+
	 * Cluster Key Fields (k) | Transaction ID (6) | Roll Pointer (7) | Non-Key Fields (j) |
	 * -----------------------+--------------------+------------------+--------------------+
	 * </pre>
	 * 
	 * User Record Extra Physical Structure:
	 *   <p>http://mysql.taobao.org/monthly/2016/02/01/
	 *   <p>https://dev.mysql.com/doc/dev/mysql-server/latest/rec_8h.html
	 * <pre>
	 * --------------------------------------------------------------------------------+
	 *                       REC_NEW_INFO_BITS(Info Flags)                             |
	 * --------------------------------------------------------+-----------------------+-----------------+--------------------+-----------------+-----------------+
	 *            1 bit       | 1 bit  |          1 bit        |        1 bit          |      4 bits     |     13 bits        |    3 bits       |    2 bytes      | 
	 * -----------------------+--------+-----------------------+-----------------------+-----------------+--------------------+-----------------+-----------------+
	 * REC_INFO_INSTANT_FLAG  | unused | REC_INFO_DELETED_FLAG | REC_INFO_MIN_REC_FLAG | REC_NEW_N_OWNED |   REC_NEW_HEAP_NO  | REC_NEW_STATUS  |   REC_NEXT      |
	 * -----------------------+--------+-----------------------+-----------------------+-----------------+--------------------+-----------------+-----------------+
	 * 
	 * REC_INFO_INSTANT_FLAG: When it is set to 1, it means this record was inserted/updated after an instant ADD COLUMN. 
	 * REC_INFO_DELETED_FLAG: 1 if record is deleted;
	 * REC_INFO_MIN_REC_FLAG: 1 if record is predefined minimum record;
	 *       REC_NEW_N_OWNED: 当该值为非0时, 表示当前记录占用page directory里一个slot, 并和前一个slot之间存在这么多个记录;
	 *       REC_NEW_HEAP_NO: 该记录的heap no;
	 *        REC_NEW_STATUS: 记录的类型
	 *                        REC_STATUS_ORDINARY = 0 (叶子节点记录), REC_STATUS_NODE_PTR=1(非叶子节点记录),
	 *                        REC_STATUS_INFIMUM = 2 (infimum记录), REC_STATUS_SUPREMUM = 3(supremum记录)
	 *             REC_NEXT : pointer to next record in page;
	 * </pre>
	 * 
	 * The next_record_offset point to the end of next record's record extra:
	 * <pre>
	 *                            +--------------+                                           
	 * +--------------------+     |   Record B   |    
	 * |     Record A       |     +--------------+
	 * +--------------------+     |   ......     |
	 * |      ......        |     | Record Extra |
	 * | next_record_offset +---> +--------------+
	 * |      ......        |     |  contents    |
	 * +--------------------+     +--------------+
	 * </pre>
	 * 
	 * 
	 * @author LiXiang
	 */
	public class ClusteredKeyLeafRecord extends RecordExtra {
		private byte[] variableFieldLengthsRaw;
		private byte[] nullFieldBitmapRaw;
		private final List<RecordField> recordFields;

		public ClusteredKeyLeafRecord() {
			recordFields = new ArrayList<RecordField>();
		}

		public byte[] getNullBitmapRaw() {
			return nullFieldBitmapRaw;
		}

		public BitSet getNullBitmap() {
			if (Objects.isNull(nullFieldBitmapRaw)) {
				return null;
			} else {
				return toBitSet(nullFieldBitmapRaw);
			}
		}

		public void setNullBitmapRaw(byte[] nullFieldBitmapRaw) {
			this.nullFieldBitmapRaw = nullFieldBitmapRaw;
		}
		
		public void addRecordField(RecordField recordField) {
			recordFields.add(recordField);
		}

		public List<RecordField> getRecordFields() {
			return recordFields;
		}

		public byte[] getVariableFieldLengthsRaw() {
			return variableFieldLengthsRaw;
		}

		public void setVariableFieldLengthsRaw(byte[] raw) {
			variableFieldLengthsRaw = raw;
		}
	}

}
