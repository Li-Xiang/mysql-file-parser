package org.littlestar.mysql.ibd.page;

import static org.littlestar.mysql.common.ParserHelper.getUInt32;
import static org.littlestar.mysql.common.ParserHelper.toHexString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * FIL_PAGE_TYPE_FSP_HDR (8)
 * 
 * @author LiXiang
 */
public class FspHdrPage extends Page {
	public static final int FSP_HEADER_START = 38;
	public static final int FSP_HEADER_END = 38 + 112;

	/** XDES Entry 0 ... 255, 40 bytes per XDES Entry, total 10240 bytes */
	public static final int XDES_ENTRY_256_START = 150;
	// public static final int XDES_ENTRY_256_END = 10240;
	public static final int XDES_ENTRY_COUNT = 256;
	public static final int XDES_ENTRY_LEN = 40;

	private final int fspHeaderStart = FSP_HEADER_START;
	private final int fspHeaderEnd = FSP_HEADER_END;

	private final int xdesEntryStart = XDES_ENTRY_256_START;

	private final FspHeader fspHeader;
	private final List<XdesEntry> xdesEntryList;

	public FspHdrPage(byte[] pageRaw, int pageSize) {
		super(pageRaw, pageSize);
		
		byte[] fspHeaderRaw =  Arrays.copyOfRange(pageRaw, fspHeaderStart, fspHeaderEnd);
		fspHeader = new FspHeader(fspHeaderRaw);
		xdesEntryList = new ArrayList<XdesEntry>();
		int from = xdesEntryStart, to = 0;
		for (int i = 0; i < XDES_ENTRY_COUNT; i++) {
			to = from + XDES_ENTRY_LEN;
			byte[] xdesEntryRaw = Arrays.copyOfRange(pageRaw, from, to);
			xdesEntryList.add(new XdesEntry(xdesEntryRaw));
			from = to;
		}
	}

	public FspHeader getFspHeader() {
		return fspHeader;
	}
	
	public byte[] getFspHeaderRaw() {
		return fspHeader.getFspHeaderRaw();
	}
	
	/**
	 * XDES(extent describe) Entry.
	 */
	public List<XdesEntry> getXdesEntryList() {
		return xdesEntryList;
	}

	/*
	@Override
	public String toString() {
		String nl = System.lineSeparator();
		StringBuilder buff = new StringBuilder();
		buff.append(getFspHeader().toString());
		for (int i = 0; i < XDES_ENTRY_COUNT; i++) {
			XdesEntry xdesEntry = xdesEntryList.get(i);
			//buff.append("XDES Entry ").append(i).append(" (" + xdesEntry.length + ")").append(": ").append(bytesToHexString(xdesEntry)).append(nl);
		}
		return buff.toString();
	}*/

	/**
	 * FSP Header:
	 *     0 -  4  space_id, Space ID (4)
	 *     4 -  8  notused, (Unused) (4)
	 *     8 -  12 fsp_size, Highest page number in file (size) (4)
	 *    12 -  16 free_limit, Highest page number initialized (free limit) (4)
	 *    16 -  20 flags, Flags (4)
	 *    20 -  24 fsp_frag_n_used, Number of pages used in "FREE_FRAG" list (4)
	 *    24 -  40 fsp_free, List base node for "FREE" list (16)
	 *    40 -  56 free_frag, List base node for "FREE_FRAG" list (16)
	 *    56 -  72 full_frag, List base node for "FULL_FRAG" list (16)
	 *    72 -  80 segid, Next Unused Segment ID (8)
	 *    80 -  96 inodes_full, List base node for "FULL_INODES" list (16)
	 *    96 - 112 inodes_free, List base node for "FREE_INODES" list (16)
	 * 
	 * Reference: 
	 * https://dev.mysql.com/doc/dev/mysql-server/latest//structfsp__header__mem__t.html
	 * 
	 * @author LiXiang
	 *
	 */
	public class FspHeader {
		private final byte[] fspHeaderRaw;
		public FspHeader(byte[] fspHeaderRaw) {
			this.fspHeaderRaw = fspHeaderRaw;
		}

		public byte[] getFspHeaderRaw() {
			return fspHeaderRaw;
		}

		public byte[] getSpaceIdRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 0, 4);
		}
		
		public long getSpaceIdUInt32() {
			return getUInt32(getSpaceIdRaw());
		}
		
		public byte[] getNotUsedRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 4, 8);
		}
		
		/**
		 * fsp_size: Highest page number in file (size) (4);
		 * 
		 * @return fsp_size
		 */
		public byte[] getFspSizeRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 8, 12);
		}
		
		/**
		 * uint32 value of fsp_size.
		 * 
		 * @see #getFspSizeRaw()
		 */
		public long getFspSizeUInt32() {
			return getUInt32(getFspSizeRaw());
		}
		
		/**
		 * free_limit: Highest page number initialized (free limit) (4)
		 * 
		 * @return free_limit
		 */
		public byte[] getFreeLimitRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 12, 16);
		}
		
		/**
		 * uint32 value of free_limit.
		 * 
		 * @see #getFreeLimitRaw()
		 */
		public long getFreeLimitUInt32() {
			return getUInt32(getFreeLimitRaw());
		}
		
		/**
		 * flags (4).
		 * @return flags
		 */
		public byte[] getFlagsRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 16, 20);
		}
		
		/**
		 * fsp_frag_n_used, Number of pages used in "FREE_FRAG" list (4).
		 * 
		 * @return fsp_frag_n_used
		 */
		public byte[] getFspFragPagesUsedRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 20, 24);
		}
		
		/**
		 * fsp_free, List base node for "FREE" list (16)
		 */
		public byte[] getFspFreeRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 24, 40);
		}
		
		/**
		 * 40 -  56, free_frag, List base node for "FREE_FRAG" list (16).
		 * 
		 * @return free_frag
		 */
		public byte[] getFreeFragRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 40, 56);
		}
		
		/**
		 * 56 -  72, full_frag, List base node for "FULL_FRAG" list (16)
		 * 
		 * @return full_frag
		 */
		public byte[] getFullFragRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 56, 72);
		}
		
		/**
		 * 72 -  80, segid, Next Unused Segment ID (8).
		 * 
		 * @return segid
		 */
		public byte[] getSegIdRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 72, 80);
		}
		
		/**
		 * 80 -  96, inodes_full, List base node for "FULL_INODES" list (16).
		 * 
		 * @return inodes_full
		 */
		public byte[] getInodesFullRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 80, 96);
		}
		
		/**
		 * 96 - 112, inodes_free, List base node for "FREE_INODES" list (16).
		 */
		public byte[] getInodesFreeRaw() {
			return Arrays.copyOfRange(fspHeaderRaw, 96, 112);
		}
		
		public String toString() {
			String nl = System.lineSeparator();
			StringBuilder buff = new StringBuilder();
			buff.append("       SPACE_ID : ").append(toHexString(getSpaceIdRaw())).append(" (").append(getSpaceIdUInt32()).append(")").append(nl)
				.append("        NOTUSED : ").append(toHexString(getNotUsedRaw())).append(nl)
				.append("       FSP_SIZE : ").append(toHexString(getFspSizeRaw())).append(" (").append(getFspSizeUInt32()).append(")").append(nl)
				.append("     FREE_LIMIT : ").append(toHexString(getFreeLimitRaw())).append(" (").append(getFreeLimitUInt32()).append(")").append(nl)
				.append("          FLAGS : ").append(toHexString(getFlagsRaw())).append(nl)
				.append("FSP_FRAG_N_USED : ").append(toHexString(getFspFragPagesUsedRaw())).append(nl)
				.append("       FSP_FREE : ").append(toHexString(getFspFreeRaw())).append(nl)
				.append("      FREE_FRAG : ").append(toHexString(getFreeFragRaw())).append(nl)
				.append("      FULL_FRAG : ").append(toHexString(getFullFragRaw())).append(nl)
				.append("          SEGID : ").append(toHexString(getSegIdRaw())).append(nl)
				.append("    INODES_FULL : ").append(toHexString(getInodesFullRaw())).append(nl)
				.append("    INODES_FREE : ").append(toHexString(getInodesFreeRaw())).append(nl)
			;
			return buff.toString();
		}

	}

	/**
	 * 
	 *  0 -  8 XDES_ID, The identifier of the segment to which this extent belongs.
	 *  8 - 20 XDES_FLST_NODE, The list node data structure for the descriptors.
	 * 20 - 24 XDES_STATE, contains state information of the extent 
	 * 24 - 40 XDES_BITMAP, Descriptor bitmap of the pages in the extent. 2 bits per page(XDES_BITS_PER_PAGE = 2), XDES_FREE_BIT=0(free), XDES_CLEAN_BIT = 1
	 * 
	 * Reference:
	 * https://dev.mysql.com/doc/dev/mysql-server/latest//group__Extent.html
	 * 
	 * @author LiXiang
	 *
	 */
	public class XdesEntry {
		private final byte[] xdesEntryRaw;

		public XdesEntry(byte[] xdesEntryRaw) {
			this.xdesEntryRaw = xdesEntryRaw;
		}

		public byte[] getXdesEntryRaw() {
			return xdesEntryRaw;
		}
		
		/**
		 * 0 - 8, XDES_ID: The identifier of the segment to which this extent belongs.
		 * 
		 * @return XDES_ID
		 */
		public byte[] getXescId() {
			return Arrays.copyOfRange(xdesEntryRaw, 0, 8); 
		}
		
		/**
		 * 8 - 20 XDES_FLST_NODE, The list node data structure for the descriptors. 
		 * 
		 * @return XDES_FLST_NODE
		 */
		public byte[] getXdesFlstNode() {
			return Arrays.copyOfRange(xdesEntryRaw, 8, 20); 
		}
		
		/**
		 * 20 - 24 XDES_STATE, contains state information of the extent.
		 * 
		 * @return XDES_STATE
		 */
		public byte[] getXdesState() {
			return Arrays.copyOfRange(xdesEntryRaw, 20, 24); 
		}
		
		/**
		 * 24 - 40 XDES_BITMAP, Descriptor bitmap of the pages in the extent. 
		 * <pre>
		 * 2 bits per page(XDES_BITS_PER_PAGE = 2):
		 *   XDES_FREE_BIT = 0 (free)
		 *   XDES_CLEAN_BIT = 1 
		 *     NOTE: currently not used! Index of the bit which tells if there are old versions of tuples on the page. 
		 * </pre>
		 * 
		 * @return XDES_BITMAP
		 */
		public byte[] getXdesBitmap() {
			return Arrays.copyOfRange(xdesEntryRaw, 24, 40); 
		}
		
		public String toString() {
			String nl = System.lineSeparator();
			StringBuilder buff = new StringBuilder();
			buff.append("       XDES_ID : ").append(toHexString(getXescId())).append(nl)
				.append("XDES_FLST_NODE : ").append(toHexString(getXdesFlstNode())).append(nl)
				.append("    XDES_STATE : ").append(toHexString(getXdesState())).append(nl)
				.append("   XDES_BITMAP : ").append(toHexString(getXdesBitmap())).append(nl)
			;
			return buff.toString();
		}
		
	}
}
