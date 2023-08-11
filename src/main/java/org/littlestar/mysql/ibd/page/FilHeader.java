package org.littlestar.mysql.ibd.page;

import static org.littlestar.mysql.common.ParserHelper.getUInt16;
import static org.littlestar.mysql.common.ParserHelper.getUInt32;

import java.util.Arrays;
import java.util.Objects;

/**
 * Fil Header, Common for all type of pages.
 * 
 * fil0types.h File Reference:
 * 
 * https://dev.mysql.com/doc/dev/mysql-server/latest/fil0types_8h.html
 * 
 * @author LiXiang
 * 
 */

public class FilHeader {
	private final byte[] headerRaw;
	
	public FilHeader(byte[] headerRaw) {
		if (Objects.isNull(headerRaw) || (headerRaw.length != 38)) {
			throw new IllegalArgumentException("page header must be 38 bytes length.");
		}
		this.headerRaw = headerRaw;
	}
	
	/**
	 * 0 - 4, FIL_PAGE_SPACE_OR_CHKSUM, The byte offsets on a file page for various variables.
	 * 
	 * MySQL-4.0.14 space id the page belongs to (== 0) but in later versions the 'new' checksum of the page.
	 * 
	 * @return FIL_PAGE_SPACE_OR_CHKSUM
	 */
	public byte[] getCheckSumRaw() {
		return Arrays.copyOfRange(headerRaw, 0, 4);
	}

	/**
	 * FIL_PAGE_OFFSET, page offset inside space.
	 * 
	 * @return FIL_PAGE_OFFSET
	 */
	public byte[] getPageOffsetRaw() {
		return Arrays.copyOfRange(headerRaw, 4, 8);
	}
	
	/** 
	 * uint32_t of FIL_PAGE_OFFSET.
	 * 
	 * @see #getPageOffsetRaw()
	 */
	public long getPageOffset() {
		return getUInt32(getPageOffsetRaw());
	}
	
	/**
	 * FIL_PAGE_PREV, offset of previous page in key order.
	 * 
	 * if there is a 'natural' predecessor of the page, its offset.
	 * 
	 * Otherwise FIL_NULL. This field is not set on BLOB pages, which are stored 
	 * as a singly-linked list. See also FIL_PAGE_NEXT.
	 */
	public byte[] getPreviousPageRaw() {
		return Arrays.copyOfRange(headerRaw, 8, 12);
	}
	
	public long getPreviousPage() {
		return getUInt32(getPreviousPageRaw());
	}

	/**
	 * FIL_PAGE_NEXT, offset of next page in key order.
	 * 
	 * if there is a 'natural' successor of the page, its offset.
	 * Otherwise FIL_NULL. B-tree index pages(FIL_PAGE_TYPE contains FIL_PAGE_INDEX) 
	 * on the same PAGE_LEVEL are maintained as a doubly linked list via FIL_PAGE_PREV 
	 * and FIL_PAGE_NEXT in the collation order of the smallest user record on each page.
	 */
	public byte[] getNextPageRaw() {
		return Arrays.copyOfRange(headerRaw, 12, 16);
	}
	
	public long getNextPage() {
		return getUInt32(getNextPageRaw());
	}

	/**
	 * FIL_PAGE_LSN, LSN(log serial number) of page's latest log record.
	 * 
	 * LSN of the end of the newest modification log record to the page.
	 */
	public byte[] getPageLSNRaw() {
		return Arrays.copyOfRange(headerRaw, 16, 24);
	}

	/**
	 * FIL_PAGE_TYPE, file page type: FIL_PAGE_INDEX,..., 2 bytes.
	 * 
	 * The contents of this field can only be trusted in the following case: 
	 * if the page is an uncompressed B-tree index page, then it is guaranteed 
	 * that the value is FIL_PAGE_INDEX. The opposite does not hold.
	 * 
	 * In tablespaces created by MySQL/InnoDB 5.1.7 or later, the contents of 
	 * this field is valid for all uncompressed pages.
	 * 
	 */
	public byte[] getPageTypeRaw() {
		return Arrays.copyOfRange(headerRaw, 24, 26);
	}
	
	/**
	 * UInt of FIL_PAGE_TYPE.
	 * 
	 * @see #getPageTypeRaw()
	 */
	public int getPageType() {
		return getUInt16(getPageTypeRaw());
	}
	
	/**
	 * Name of FIL_PAGE_TYPE.
	 * 
	 * @see #getPageType(int)
	 * @see #getPageTypeRaw()
	 */
	public String getPageTypeName() {
		int type = getPageType();
		return getPageType(type);
	}

	/**
	 * FIL_PAGE_FILE_FLUSH_LSN, the file has been flushed to disk 
	 * at least up this LSN, valid only on the first page of the file.
	 * 
	 * this is only defined for the first page of the system tablespace: 
	 * the file has been flushed to disk at least up to this LSN.
	 * 
	 * For FIL_PAGE_COMPRESSED pages, we store the compressed page control 
	 * information in these 8 bytes.
	 * 
	 * @return
	 */
	public byte[] getFlushLSNRaw() {
		return Arrays.copyOfRange(headerRaw, 26, 34);
	}

	/**
	 * FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, starting from 4.1.x this contains the space id of the page.
	 * 
	 * @return FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID
	 */
	public byte[] getSpaceIdRaw() {
		return Arrays.copyOfRange(headerRaw, 34, 38);
	}
	
	/**
	 * UInt value of FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID.
	 * 
	 * @see #getSpaceIdRaw()
	 */
	public long getSpaceId() {
		return getUInt32(getSpaceIdRaw());
	}
	
	public static final int FIL_PAGE_INDEX = 17855;
	public static final int FIL_PAGE_RTREE = 17854;
	public static final int FIL_PAGE_SDI = 17853;
	public static final int FIL_PAGE_TYPE_UNUSED = 1;
	public static final int FIL_PAGE_UNDO_LOG = 2;
	public static final int FIL_PAGE_INODE = 3;
	public static final int FIL_PAGE_IBUF_FREE_LIST = 4;
	public static final int FIL_PAGE_TYPE_ALLOCATED = 0;
	public static final int FIL_PAGE_IBUF_BITMAP = 5;
	public static final int FIL_PAGE_TYPE_SYS = 6;
	public static final int FIL_PAGE_TYPE_TRX_SYS = 7;
	public static final int FIL_PAGE_TYPE_FSP_HDR = 8;
	public static final int FIL_PAGE_TYPE_XDES = 9;
	public static final int FIL_PAGE_TYPE_BLOB = 10;
	public static final int FIL_PAGE_TYPE_ZBLOB = 11;
	public static final int FIL_PAGE_TYPE_ZBLOB2 = 12;
	public static final int FIL_PAGE_TYPE_UNKNOWN = 13;
	public static final int FIL_PAGE_COMPRESSED = 14;
	public static final int FIL_PAGE_ENCRYPTED = 15;
	public static final int FIL_PAGE_COMPRESSED_AND_ENCRYPTED = 16;
	public static final int FIL_PAGE_ENCRYPTED_RTREE = 17;
	public static final int FIL_PAGE_SDI_BLOB = 18;
	public static final int FIL_PAGE_SDI_ZBLOB = 19;
	public static final int FIL_PAGE_TYPE_LEGACY_DBLWR = 20;
	public static final int FIL_PAGE_TYPE_RSEG_ARRAY = 21;
	public static final int FIL_PAGE_TYPE_LOB_INDEX = 22;
	public static final int FIL_PAGE_TYPE_LOB_DATA = 23;
	public static final int FIL_PAGE_TYPE_LOB_FIRST = 24;
	public static final int FIL_PAGE_TYPE_ZLOB_FIRST = 25;
	public static final int FIL_PAGE_TYPE_ZLOB_DATA = 26;
	public static final int FIL_PAGE_TYPE_ZLOB_INDEX = 27;
	public static final int FIL_PAGE_TYPE_ZLOB_FRAG = 28;
	public static final int FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY = 29;
	public static final int FIL_PAGE_TYPE_LAST = 29;
	/**
	 * fil0fil.h: https://dev.mysql.com/doc/dev/mysql-server/latest/fil0fil_8h_source.html
	 */
	public static String getPageType(int type) {
		switch(type) {
		case FIL_PAGE_INDEX:                    return "FIL_PAGE_INDEX";
		case FIL_PAGE_RTREE:                    return "FIL_PAGE_RTREE";
		case FIL_PAGE_SDI:                      return "FIL_PAGE_SDI";
		case FIL_PAGE_TYPE_UNUSED:              return "FIL_PAGE_TYPE_UNUSED";
		case FIL_PAGE_UNDO_LOG:                 return "FIL_PAGE_UNDO_LOG";
		case FIL_PAGE_INODE:                    return "FIL_PAGE_INODE";
		case FIL_PAGE_IBUF_FREE_LIST:           return "FIL_PAGE_IBUF_FREE_LIST";
		case FIL_PAGE_TYPE_ALLOCATED:           return "FIL_PAGE_TYPE_ALLOCATED";
		case FIL_PAGE_IBUF_BITMAP:              return "FIL_PAGE_IBUF_BITMAP";
		case FIL_PAGE_TYPE_SYS:                 return "FIL_PAGE_TYPE_SYS";
		case FIL_PAGE_TYPE_TRX_SYS:             return "FIL_PAGE_TYPE_TRX_SYS";
		case FIL_PAGE_TYPE_FSP_HDR:             return "FIL_PAGE_TYPE_FSP_HDR";
		case FIL_PAGE_TYPE_XDES:                return "FIL_PAGE_TYPE_XDES";
		case FIL_PAGE_TYPE_BLOB:                return "FIL_PAGE_TYPE_BLOB";
		case FIL_PAGE_TYPE_ZBLOB:               return "FIL_PAGE_TYPE_ZBLOB";
		case FIL_PAGE_TYPE_ZBLOB2:              return "FIL_PAGE_TYPE_ZBLOB2";
		case FIL_PAGE_TYPE_UNKNOWN:             return "FIL_PAGE_TYPE_UNKNOWN";
		case FIL_PAGE_COMPRESSED:               return "FIL_PAGE_COMPRESSED";
		case FIL_PAGE_ENCRYPTED:                return "FIL_PAGE_ENCRYPTED";
		case FIL_PAGE_COMPRESSED_AND_ENCRYPTED: return "FIL_PAGE_COMPRESSED_AND_ENCRYPTED";
		case FIL_PAGE_ENCRYPTED_RTREE:          return "FIL_PAGE_ENCRYPTED_RTREE";
		case FIL_PAGE_SDI_BLOB:                 return "FIL_PAGE_SDI_BLOB";
		case FIL_PAGE_SDI_ZBLOB:                return "FIL_PAGE_SDI_ZBLOB";
		case FIL_PAGE_TYPE_LEGACY_DBLWR:        return "FIL_PAGE_TYPE_LEGACY_DBLWR";
		case FIL_PAGE_TYPE_RSEG_ARRAY:          return "FIL_PAGE_TYPE_RSEG_ARRAY";
		case FIL_PAGE_TYPE_LOB_INDEX:           return "FIL_PAGE_TYPE_LOB_INDEX";
		case FIL_PAGE_TYPE_LOB_DATA:            return "FIL_PAGE_TYPE_LOB_DATA";
		case FIL_PAGE_TYPE_LOB_FIRST:           return "FIL_PAGE_TYPE_LOB_FIRST";
		case FIL_PAGE_TYPE_ZLOB_FIRST:          return "FIL_PAGE_TYPE_ZLOB_FIRST";
		case FIL_PAGE_TYPE_ZLOB_DATA:           return "FIL_PAGE_TYPE_ZLOB_DATA";
		case FIL_PAGE_TYPE_ZLOB_INDEX:          return "FIL_PAGE_TYPE_ZLOB_INDEX";
		case FIL_PAGE_TYPE_ZLOB_FRAG:           return "FIL_PAGE_TYPE_ZLOB_FRAG";
		case FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY:     return "FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY|FIL_PAGE_TYPE_LAST";
		}
		return Integer.toString(type);
	}
}
