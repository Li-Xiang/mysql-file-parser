package org.littlestar.mysql.ibd.page;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import org.littlestar.mysql.ibd.parser.ColumnMeta;

import static org.littlestar.mysql.common.ParserHelper.*;
import static org.littlestar.mysql.ibd.parser.ColumnType.*;

/**
 * FIL_PAGE_INDEX (17855).
 * 
 * <pre>
 * 0----->+----------------------+
 *        | FIL Header (38)      |
 * 38---->+----------------------+--------------------+
 *        | INDEX Header (36)    |                    |
 * 74---->+----------------------+  PAGE Header (56)  |
 *        | FSEG Header (20)     |                    |
 * 94---->+----------------------+--------------------+
 *        | System Records (26)  | Infimum + Supremum |
 * 120--->+----------------------+--------------------+
 *        | User Records         |
 *        +----------------------+
 *        | Free Space           |
 *        +----------------------+
 *        | Page Directory       |
 * 16376->+----------------------+
 *        | FIL Trailer (8)      |
 * 16384->+----------------------+
 * 
 * Note:
 * 1. INDEX_HEADER + FSEG_HEADER a.k.a. PAGE_HEADER (36 + 20 = 56);
 * 2. USER_RECORDS from SYSTEM_RECORDS to PAGE_HEAP_TOP;
 * 3. PAGE_DIRECTORY, grows downwards from FIL_TRAILER；
 * </pre>
 * 
 * @author LiXiang
 *
 */

public class IndexPage extends Page {
	public static final int INDEX_HEADER_START_POS = 38; // inclusive
	public static final int INDEX_HEADER_END_POS   = 74; // exclusive
	
	public static final int FSEG_HEADER_START_POS  = 74; // inclusive
	public static final int FSEG_HEADER_END_POS    = 94; // exclusive
	
	public static final int SYSTEM_RECORDS_START_POS  = 94;  
	public static final int SYSTEM_RECORDS_END_POS    = 120; // exclusive
	public static final int INFIMUM_EXTRA_START_POS   = 94;
	public static final int INFIMUM_EXTRA_END_POS     = 99;
	public static final int SUPREMUM_EXTRA_START_POS  = 107; 
	public static final int SUPREMUM_EXTRA_END_POS    = 112; 
	
	public static final int REC_N_NEW_EXTRA_BYTES     = 5;
	public final int maxRecs;

	public IndexPage(byte[] pageRaw, int pageSize) {
		super(pageRaw, pageSize);
		maxRecs = pageSize / 5;
	}

	public IndexPage(byte[] pageRaw) {
		this(pageRaw, DEFAULT_PAGE_SIZE);
	}

	public IndexHeader getIndexHeader() {
		return new IndexHeader();
	}

	public byte[] getIndexHeaderRaw() {
		return Arrays.copyOfRange(pageRaw, INDEX_HEADER_START_POS, INDEX_HEADER_END_POS);
	}

	public FsegHeader getFsegHeader() {
		return new FsegHeader();
	}

	public byte[] getFsegHeaderRaw() {
		return Arrays.copyOfRange(pageRaw, FSEG_HEADER_START_POS, FSEG_HEADER_END_POS);
	}

	public SystemRecords getSystemRecords() {
		return new SystemRecords();
	}

	public byte[] getSystemRecordsRaw() {
		return Arrays.copyOfRange(pageRaw, SYSTEM_RECORDS_START_POS, SYSTEM_RECORDS_END_POS);
	}

	/**
	 * the bytes from SYSTEM_RECORDS to PAGE_HEAP_TOP in this page.
	 * 
	 * @return the bytes of user records.
	 */
	public byte[] getUserRecordsRaw() {
		int startPos = SYSTEM_RECORDS_END_POS;
		int endPos = getIndexHeader().getHeapTopPosition();
		return Arrays.copyOfRange(pageRaw, startPos, endPos);
	}

	public List<RecordExtra> getRecordsExtra() {
		int infimumRecNextPos = getSystemRecords().getInfimumNextRecordPos();
		int currentPos = infimumRecNextPos;
		List<RecordExtra> records = new ArrayList<RecordExtra>();
		while (currentPos > SUPREMUM_EXTRA_END_POS) {
			int extraEndPos = currentPos;
			int extraStartPos = currentPos - IndexPage.REC_N_NEW_EXTRA_BYTES;//  5 bytes for DYNAMIC
			byte[] extraRaw = Arrays.copyOfRange(pageRaw, extraStartPos, extraEndPos);
			RecordExtra extra = new RecordExtra(extraRaw);
			int nextOffset = extra.getNextRecordOffset();
			int nextPos = nextOffset + extraEndPos;
			extra.setNextRecordPos(nextPos);
			records.add(extra);
			if (nextPos == SUPREMUM_EXTRA_END_POS) {
				break;
			}
			currentPos = nextPos;
		}
		return records;
	}
	
	/**
	 * 
	 * slots[n] ... slots[2] slots[1] slots[0]
	 * 
	 * @return
	 */
	public List<byte[]> getPageDirectorySlotsRaw() {
		int slotCount = getIndexHeader().getDirectorySlotCount();
		//// 2 bytes/slot
		List<byte[]> directorySlots = new ArrayList<byte[]>();
		int offset = getPageSize() - PAGE_TRAILER_LENGTH;
		for (int i = 0; i < slotCount; i++) {
			int to = offset;
			int from = offset - 2;
			byte[] d = Arrays.copyOfRange(pageRaw, from, to);
			offset = from;
			directorySlots.add(d);
		}
		return directorySlots;
	}
	
	/**
	 * INDEX Header / Index Page Header (36).
	 * 
	 * page0types.h File Reference:
	 * 
	 * https://dev.mysql.com/doc/dev/mysql-server/latest/page0types_8h.html
	 * 
	 * @author LiXiang
	 *
	 */
	public class IndexHeader {
		/**
		 * 38 - 40, number of directory slots(2).
		 * 
		 * PAGE_N_DIR_SLOTS, number of slots in page directory.
		 * 
		 * @return number of directory slots.
		 */
		public byte[] getDirectorySlotCountRaw() {
			return Arrays.copyOfRange(pageRaw, 38, 40);
		}

		public int getDirectorySlotCount() {
			return getUInt16(getDirectorySlotCountRaw());
		}

		/**
		 * 40 - 42, heap top position(2).
		 * 
		 * PAGE_HEAP_TOP, pointer to record heap top.
		 * 
		 * @return heap top position.
		 */
		public byte[] getHeapTopPositionRaw() {
			return Arrays.copyOfRange(pageRaw, 40, 42);
		}

		public int getHeapTopPosition() {
			return getUInt16(getHeapTopPositionRaw());
		}

		/**
		 * 42 - 44, number of heap records/format flag(2).
		 * 
		 * <p>
		 * PAGE_N_HEAP, The total number of records in the page, including the infimum
		 * and supremum system records, and garbage (deleted) records.
		 * 
		 * <p>
		 * Format Flag, The format of the records in this page, stored in the high bit
		 * (0x8000) of the "Number of Heap Records" field. Two values are possible:
		 * COMPACT and REDUNDANT.
		 * 
		 * 15(bit)=flag: new-style compact page format.
		 * 
		 * @return number of heap records/format flag.
		 */
		public byte[] getHeapRecordsRaw() {
			return Arrays.copyOfRange(pageRaw, 42, 44);
		}

		/**
		 * The total number of records in the page.
		 * 
		 * @see #getHeapRecordsRaw()
		 * @return The total number of records in the page
		 */
		public int getHeapRecords() {
			int records = getInt16(getHeapRecordsRaw());
			return records & 0x7FFF;
		}

		/**
		 * The format of the records in this page: true = new-style compact page format.
		 * @see #getHeapRecordsRaw()
		 * @return 
		 */
		public boolean isNewStyleCompactFormat() {
			byte b = getHeapRecordsRaw()[0];
			int flag = getUInt8(b) >> 7;
			return flag == 1 ? true : false;
		}
		
		/**
		 * 44 - 46, first garbage record offset(2).
		 * 
		 * <p>PAGE_FREE, pointer to start of page free record list.
		 * 
		 * <p>A pointer to the first entry in the list of garbage (deleted) records. The
		 * list is singly-linked together using the "next record" pointers in each
		 * record header. (This is called "free" within InnoDB, but this name is
		 * somewhat confusing.)
		 * 
		 * @return first garbage record offset.
		 */
		public byte[] getFirstGarbageOffsetRaw() {
			return Arrays.copyOfRange(pageRaw, 44, 46);
		}

		public int getFirstGarbageOffset() {
			byte[] nextRecordOffset = getFirstGarbageOffsetRaw();
			return (nextRecordOffset[0] << 8 | nextRecordOffset[1] & 0xFF);
		}

		public int getFirstGarbageOffsetInPage() {
			int nextRecordOffset = getFirstGarbageOffset();
			int currPos = INDEX_HEADER_START_POS + 8;
			return currPos + nextRecordOffset;
		}
		
		/**
		 * 46 - 48, garbage space(2).
		 * 
		 * PAGE_GARBAGE, number of bytes in deleted records.
		 * 
		 * @return garbage space.
		 */
		public byte[] getGarbageBytesRaw() {
			return Arrays.copyOfRange(pageRaw, 46, 48);
		}

		public int getGarbageBytes() {
			byte[] bytes = getGarbageBytesRaw();
			return getUInt16(bytes);
		}
		
		/**
		 * 48 - 50, last insert position(2).
		 * 
		 * PAGE_LAST_INSERT, pointer to the last inserted record, or NULL if this info
		 * has been reset by a delete, for example.
		 * 
		 * @return last insert position.
		 */
		public byte[] getLastInsertPositionRaw() {
			return Arrays.copyOfRange(pageRaw, 48, 50);
		}

		/**
		 * 50 - 52, page direction(2).
		 * 
		 * PAGE_DIRECTION, last insert direction: PAGE_LEFT(0x01), PAGE_RIGHT(0x02), PAGE_NO_DIRECTION(0x05)
		 * 
		 * @return page direction.
		 */

		public byte[] getPageDirectionRaw() {
			return Arrays.copyOfRange(pageRaw, 50, 52);
		}

		/**
		 * 52 - 54, number of inserts in page direction(2).
		 * 
		 * PAGE_N_DIRECTION, number of consecutive inserts to the same direction
		 * 
		 * @return number of inserts in page direction.
		 */
		public byte[] getInsertsInPageDirectionRaw() {
			return Arrays.copyOfRange(pageRaw, 52, 54);
		}

		/**
		 * 54 - 56, number of records(2).
		 * 
		 * PAGE_N_RECS, number of user records on the page.
		 * 
		 * @return number of records.
		 */
		public byte[] getPageRecordsRaw() {
			return Arrays.copyOfRange(pageRaw, 54, 56);
		}

		/**
		 * 56 - 64, maximum transaction id(8).
		 * 
		 * PAGE_MAX_TRX_ID, highest id of a trx which may have modified a record on the
		 * page; trx_id_t; defined only in secondary indexes and in the insert buffer
		 * tree.
		 * 
		 * @return maximum transaction id.
		 */
		public byte[] getMaxTransactionIdRaw() {
			return Arrays.copyOfRange(pageRaw, 56, 64);
		}

		/**
		 * 64 - 66, page level(2).
		 * 
		 * PAGE_LEVEL, level of the node in an index tree; the leaf level is the level
		 * 0. This field should not be written to after page creation.
		 * 
		 * @return page level.
		 */
		public byte[] getPageLevelRaw() {
			return Arrays.copyOfRange(pageRaw, 64, 66);
		}
		
		/**
		 * get integer value of PAGE_LEVEL.
		 * 
		 * @see #getPageLevelRaw()
		 * @return integer value of page level
		 */
		public int getPageLevel() {
			return getUInt16(getPageLevelRaw());
		}

		/**
		 * 66 - 74, index id(8). PAGE_INDEX_ID, index id where the page belongs. This
		 * field should not be written to after page creation.
		 * 
		 * @return index id.
		 */
		public byte[] getIndexIdRaw() {
			return Arrays.copyOfRange(pageRaw, 66, 74);
		}
		
		public BigInteger getIndexId() {
			return getUInt64(getIndexIdRaw());
		}
	}

	/**
	 * FSEG Header (20) = PAGE_BTR_SEG_LEAF(10) + PAGE_BTR_SEG_TOP(10).
	 * 
	 * 
	 * @author LiXiang
	 *
	 */
	public class FsegHeader {

		/**
		 * 74 - 84, PAGE_BTR_SEG_LEAF(10), file segment header for the leaf pages in a
		 * B-tree: defined only on the root page of a B-tree, but not in the root of an
		 * ibuf tree.
		 * 
		 * @return PAGE_BTR_SEG_LEAF
		 */
		public byte[] getPageBtrSegLeafRaw() {
			return Arrays.copyOfRange(pageRaw, 74, 84);
		}

		/**
		 * 84 - 94, PAGE_BTR_SEG_TOP (10), 36 + FSEG_HEADER_SIZE. FSEG_HEADER_SIZE,
		 * Length of the file system header, in bytes.
		 * 
		 * @return PAGE_BTR_SEG_TOP
		 */
		public byte[] getPageBtrSegTopRaw() {
			return Arrays.copyOfRange(pageRaw, 84, 94);
		}

		/**
		 * 74 - 78, Leaf Pages Inode Space ID(4).
		 * 
		 * @return Leaf Pages Inode Space ID.
		 */
		public byte[] getLeafPagesInodeSpaceId() {
			return Arrays.copyOfRange(pageRaw, 74, 78);
		}

		/**
		 * 78 - 82, Leaf Pages Inode Page Number (4).
		 * 
		 * @return Leaf Pages Inode Page Number.
		 */
		public byte[] getLeafPagesInodePageNumber() {
			return Arrays.copyOfRange(pageRaw, 78, 82);
		}

		/**
		 * 82 - 84, Leaf Pages Inode Offset (2).
		 * 
		 * @return Leaf Pages Inode Offset.
		 */
		public byte[] getLeafPagesInodeOffset() {
			return Arrays.copyOfRange(pageRaw, 82, 84);
		}

		/**
		 * 84 - 88, Internal (non-leaf) Inode Space ID (4).
		 * 
		 * @return Internal (non-leaf) Inode Space ID.
		 */
		public byte[] getInternalInodeSpaceId() {
			return Arrays.copyOfRange(pageRaw, 84, 88);
		}

		/**
		 * 88 - 92, Internal (non-leaf) Inode Page Number (4).
		 * 
		 * @return Internal (non-leaf) Inode Page Number.
		 */
		public byte[] getInternalInodePageNumber() {
			return Arrays.copyOfRange(pageRaw, 88, 92);
		}

		/**
		 * 92 - 94, Internal (non-leaf) Inode Offset (2).
		 * 
		 * @return Internal (non-leaf) Inode Offset.
		 */
		public byte[] getInternalInodeOffset() {
			return Arrays.copyOfRange(pageRaw, 92, 94);
		}
	}
	
	/**
	 * <pre>
	 * System Records (26) = Infimum Record + Supremum Record.
	 * 
	 * 94 -----> +-----------------------------------+---------------------------
	 *           | Info Flags              (4 bits)  |
	 *           | Number of Records Owned (4 bits)  |
	 * 95 -----> +-----------------------------------+
	 *           | Order                   (13 bits) |
	 *           | Record Type             (3  bits) |
	 * 97 -----> +-----------------------------------+
	 *           | Next Record Offset      (2 bytes) |
	 * 99 -----> +-----------------------------------+
	 *           | "infimum "              (8 bytes) |
	 * 107 ----> +-----------------------------------+
	 *           | Info Flags              (4 bits)  |
	 *           | Number of Records Owned (4 bits)  |
	 * 108 ----> +-----------------------------------+
	 *           | Order                   (13 bits) |
	 *           | Record Type             (3  bits) |
	 * 110 ----> +-----------------------------------+
	 *           | Next Record Offset      (2 bytes) |
	 * 112 ----> +-----------------------------------+
	 *           | "supremum"              (8 bytes) |
	 * 120 ----> +-----------------------------------+
	 * </pre>
	 * 
	 * @author LiXiang
	 */
	public class SystemRecords {
		private final RecordExtra infimumExtra;
		private final RecordExtra supremumExtra;

		public SystemRecords() {
			byte[] infimumExtraRaw = Arrays.copyOfRange(pageRaw, INFIMUM_EXTRA_START_POS, INFIMUM_EXTRA_END_POS);
			infimumExtra = new RecordExtra(infimumExtraRaw);
			int nextRecordOffset = getInfimumExtra().getNextRecordOffset();
			infimumExtra.setNextRecordPos(nextRecordOffset + INFIMUM_EXTRA_END_POS);
			byte[] supremumExtraRaw = Arrays.copyOfRange(pageRaw, SUPREMUM_EXTRA_START_POS, SUPREMUM_EXTRA_END_POS);
			supremumExtra = new RecordExtra(supremumExtraRaw);
			nextRecordOffset = getSupremumExtra().getNextRecordOffset();
			supremumExtra.setNextRecordPos(nextRecordOffset + SUPREMUM_EXTRA_END_POS);
		}

		public RecordExtra getInfimumExtra() {
			return infimumExtra;
		}

		/**
		 * The offset of next record in page.
		 * 
		 * @return
		 */
		public int getInfimumNextRecordPos() {
			return infimumExtra.getNextRecordPos();
			/*
			int nextRecordOffset = getInfimumExtra().getNextRecordOffset();
			int currPos = INFIMUM_EXTRA_END_POS;
			return currPos + nextRecordOffset;*/
		}
		
		/**
		 * 99 - 107, content = "infimum" (8)
		 * 
		 * @return the bytes of string "infimum"
		 */
		public byte[] getInfimumContentRaw() {
			return Arrays.copyOfRange(pageRaw, 99, 107);
		}

		public RecordExtra getSupremumExtra() {
			return supremumExtra;
		}

		public int getSupremumNextRecordPos() {
			return supremumExtra.getNextRecordPos();
			/*
			int nextRecordOffset = getSupremumExtra().getNextRecordOffset();
			int currPos = SUPREMUM_EXTRA_END_POS;
			return currPos + nextRecordOffset;*/
		}
		
		/**
		 * 112 - 120, content: "supremum" (8)
		 * 
		 * @return "supremum"
		 */
		public byte[] getSupremumContentRaw() {
			return Arrays.copyOfRange(pageRaw, 112, 120);
		}
	}
	
	/**
	 * 当前记录(row)对应列的数据, 相比ColumnMeta多了记录内容(content); 该记录字段是否为空; 该记录的字段实际存储长度(bytes)。
	 */
	public class RecordField extends ColumnMeta {
		private byte[] contentRaw;
		private boolean isNull = false;

		public byte[] getConetentRaw() {
			return contentRaw;
		}

		public RecordField() {}

		public RecordField(ColumnMeta meta) {
			setPos(meta.getPos());
			setName(meta.getName());
			setType(meta.getType());
			setNullable(meta.isNullable());
			setVariable(meta.isVariableLength());
			setInternal(meta.isInternal());
			setLength(meta.getLength());
			setPrecision(meta.getPrecision());
			setScale(meta.getScale());
			setEnumMembers(meta.getEnumMembers());
		}

		/**
		 * the store bytes of the record field content in page.
		 */
		@Override
		public RecordField setLength(int length) {
			super.setLength(length);
			return this;
		}

		public void setConetentRaw(byte[] contentRaw) {
			this.contentRaw = contentRaw;
		}

		public Object getContent() {
			if (Objects.isNull(contentRaw)) return null;
			final byte[] raw = contentRaw.clone();
			switch (getType()) {
			/** Integer Types (Exact Value) - INTEGER, INT, SMALLINT, TINYINT, MEDIUMINT, BIGINT */
			case TINYINT           : return getIntValue(raw);
			case UNSIGNED_TINYINT  : return getUIntValue(raw);
			
			case SMALLINT          : return getIntValue(raw);
			case UNSIGNED_SMALLINT : return getUIntValue(raw);
			
			case MEDIUMINT         : return getIntValue(raw);
			case UNSIGNED_MEDIUMINT: return getUIntValue(raw);
			
			case INT               : return getIntValue(raw);
			case UNSIGNED_INT      : return getUIntValue(raw);
			
			case BIGINT            : return getIntValue(raw);
			case UNSIGNED_BIGINT   : return getUIntValue(raw);
			
			/** Integer Types (Exact Value) */
			
			/** Floating-Point Number  **/
			case FLOAT             : return getFloatValue(raw);
			case DOUBLE            : return getDoubleValue(raw);
			/** Floating-Point Number  **/
			
			case DECIMAL:
				int precision = getPrecision();
				int scale = getScale();
				return getDecimalValue(raw, precision, scale);
				
			/** String Type Storage */
			case ENUM:
				int size = raw.length;
				int index = 0;
				if (size == 1) {
					index = getUInt8(raw[0]);
				} else if (size == 2) {
					index = getUInt16(raw);
				} else {
					return "unsupport enum length: " + size;
				}
				return getEnumMember(index);
			/**
			 * The size of a SET object is determined by the number of different set members. 
			 * If the set size is N, the object occupies (N+7)/8 bytes, rounded up to 1, 2, 3, 4, or 8 bytes. 
			 * A SET can have a maximum of 64 members.
			 * 使用位图表示是否包含某个成员, 8字节有64位, 所以最大支持64个SET成员。
			 */
			case SET:
				ArrayList<String> list = new ArrayList<String>();
				BitSet bs = toBitSet(raw);
				for (int i = 0; i < getEnumMembers().size(); i++) {
					if (bs.get(i)) {
						list.add(getEnumMember(i + 1));
					}
				}
				return list;
			case TEXT:
				return new String(raw);
			case VARCHAR:
				return new String(raw);
			case CHAR: return new String(raw);
			case YEAR:
				int yearValue = getUInt8(raw[0]);
				yearValue += 1900;
				return yearValue;
			case DATETIME:
				return getDateTimeV2(raw);
			case TIMESTAMP:
				return getTimestampValue(raw);
			default:
				if (Objects.isNull(raw)) {
					return raw;
				} else {
					return toHexString(raw);
				}
			}
		}

		public boolean isNull() {
			return isNull;
		}

		public void setNull(boolean isNull) {
			this.isNull = isNull;
		}
		
	}
	
}
