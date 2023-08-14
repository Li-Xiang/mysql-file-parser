package org.littlestar.mysql.ibd.parser;

import static org.littlestar.mysql.common.ParserHelper.*;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.littlestar.mysql.ibd.page.IndexPage.FsegHeader;
import org.littlestar.mysql.ibd.page.IndexPage.IndexHeader;
import org.littlestar.mysql.ibd.page.IndexPage.RecordField;
import org.littlestar.mysql.ibd.page.SecondaryKeyLeafPage.SecondaryKeyLeafRecord;
import org.littlestar.mysql.ibd.page.SecondaryKeyNonLeafPage.SecondaryKeyNonLeafRecord;
import org.littlestar.mysql.ibd.page.IndexPage.SystemRecords;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage;
import org.littlestar.mysql.ibd.page.ClusteredKeyNonLeafPage;
import org.littlestar.mysql.ibd.page.FilHeader;
import org.littlestar.mysql.ibd.page.IndexPage;
import org.littlestar.mysql.ibd.page.Page;
import org.littlestar.mysql.ibd.page.RecordExtra;
import org.littlestar.mysql.ibd.page.SecondaryKeyLeafPage;
import org.littlestar.mysql.ibd.page.SecondaryKeyNonLeafPage;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage.ClusteredKeyLeafRecord;
import org.littlestar.mysql.ibd.page.ClusteredKeyNonLeafPage.ClusteredKeyNonLeafRecord;

public class PageHelper {
	private static final String NL = System.lineSeparator();
	
	public static String dumpPages(IbdFileParser ibdFileParser) throws IOException {
		StringBuilder buff = new StringBuilder();
		Map<Integer, List<Long>> pageTypeMap = ibdFileParser.getPageTypeMap();
		for (Integer pageType : pageTypeMap.keySet()) {
			List<Long> pageIndexes = pageTypeMap.get(pageType);
			buff.append(FilHeader.getPageType(pageType)).append(":");
			//// 格式化输出页索引, 更好的显示效果。
			int count = 0, rowlen = 10;
			for (long index : pageIndexes) {
				count++;
				buff.append(String.format("%-6d", index));
				if (count % rowlen == 0)
					buff.append(NL);
			}
			buff.append(NL);
		}
		return buff.toString();
	}
	
	public static String dumpIndexPages(IbdFileParser parser) throws IOException {
		StringBuilder buff = new StringBuilder();
		Map<Integer, List<Long>> pageMap = parser.getPageTypeMap();
		List<Long> pageIndexes = pageMap.get(FilHeader.FIL_PAGE_INDEX);
		for (long index : pageIndexes) {
			Page page = parser.getPage(index);
			if(page instanceof IndexPage) {
				IndexPage indexPage = (IndexPage) page;
				IndexHeader indexHeader = indexPage.getIndexHeader();
				//PageMeta pageMeta = ibdFileParser.getPageMeta(index);
				buff.append(" Page#: ").append(index)
					//.append("      Range : ").append(pageMeta.getPageStartPos()).append(" - ").append(pageMeta.getPageStartPos()).append(NL)
					.append(" Index ID : ").append(indexHeader.getIndexId())
					.append(" Page Level : ").append(indexHeader.getPageLevel()).append(NL);
			}
		}
		return buff.toString();
	}
	
	public static String dumpFilHeader(final FilHeader header) {
		StringBuilder buff = new StringBuilder();
		buff.append("                CHECKSUM : ").append(toHexString(header.getCheckSumRaw())).append(NL) //FIL_PAGE_SPACE_OR_CHKSUM
			.append("         FIL_PAGE_OFFSET : ").append(toHexString(header.getPageOffsetRaw())).append(" (").append(header.getPageOffset()).append(")").append(NL)
			.append("           FIL_PAGE_PREV : ").append(toHexString(header.getPreviousPageRaw())).append(NL)
			.append("           FIL_PAGE_NEXT : ").append(toHexString(header.getNextPageRaw())).append(NL)
			.append("            FIL_PAGE_LSN : ").append(toHexString(header.getPageLSNRaw())).append(NL)
			.append("           FIL_PAGE_TYPE : ").append(toHexString(header.getPageTypeRaw())).append(" (").append(header.getPageType()).append(", ").append(header.getPageTypeName()).append(")").append(NL)
			.append(" FIL_PAGE_FILE_FLUSH_LSN : ").append(toHexString(header.getFlushLSNRaw())).append(NL)
			.append("                SPACE_ID : ").append(toHexString(header.getSpaceIdRaw())).append(" (").append(header.getSpaceId()).append(")").append(NL);//FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID
		return buff.toString();
	}
	
	public static String dumpIndexHeader(IndexHeader header) {
		StringBuilder buff = new StringBuilder();
		buff.append(" PAGE_N_DIR_SLOTS : ").append(toHexString(header.getDirectorySlotCountRaw())).append(NL)
			.append("    PAGE_HEAP_TOP : ").append(toHexString(header.getHeapTopPositionRaw())).append(NL)
			.append("      PAGE_N_HEAP : ").append(toHexString(header.getHeapRecordsRaw())).append(" (records=").append(header.getHeapRecords()).append(" | new-style-format=").append(header.isNewStyleCompactFormat()).append(")").append(NL)
			.append("        PAGE_FREE : ").append(toHexString(header.getFirstGarbageOffsetRaw())).append(NL)
			.append("     PAGE_GARBAGE : ").append(toHexString(header.getGarbageBytesRaw())).append(NL)
			.append(" PAGE_LAST_INSERT : ").append(toHexString(header.getLastInsertPositionRaw())).append(NL)
			.append("   PAGE_DIRECTION : ").append(toHexString(header.getPageDirectionRaw())).append(" {PAGE_LEFT(0x01), PAGE_RIGHT(0x02), PAGE_NO_DIRECTION(0x05)}").append(NL)
			.append(" PAGE_N_DIRECTION : ").append(toHexString(header.getInsertsInPageDirectionRaw())).append(NL)
			.append("      PAGE_N_RECS : ").append(toHexString(header.getPageRecordsRaw())).append(NL)
			.append("  PAGE_MAX_TRX_ID : ").append(toHexString(header.getMaxTransactionIdRaw())).append(NL)
			.append("       PAGE_LEVEL : ").append(toHexString(header.getPageLevelRaw())).append(NL)
			.append("    PAGE_INDEX_ID : ").append(toHexString(header.getIndexIdRaw())).append(NL);
		return buff.toString();
	}

	public static String dumpFsegHeader(FsegHeader header) {
		StringBuilder buff = new StringBuilder();
		buff.append("         Leaf Pages Inode Space ID : ").append(toHexString(header.getLeafPagesInodeSpaceId())).append(NL)
			.append("            Leaf Pages Inode Page# : ").append(toHexString(header.getLeafPagesInodePageNumber())).append(NL)
			.append("           Leaf Pages Inode Offset : ").append(toHexString(header.getLeafPagesInodeOffset())).append(NL)
			.append("Internal (non-leaf) Inode Space ID : ").append(toHexString(header.getInternalInodeSpaceId())).append(NL)
			.append("   Internal (non-leaf) Inode Page# : ").append(toHexString(header.getInternalInodePageNumber())).append(NL)
			.append("  Internal (non-leaf) Inode Offset : ").append(toHexString(header.getInternalInodeOffset())).append(NL)
		;
		/*
		buff.append("PAGE_BTR_SEG_LEAF : ").append(toHexString(header.getPageBtrSegLeafRaw())).append(NL)
			.append(" PAGE_BTR_SEG_TOP : ").append(toHexString(header.getPageBtrSegTopRaw())).append(NL);
		*/
		return buff.toString();
	}
	
	public static String dumpRecordExtra(RecordExtra extra) {
		StringBuilder buff = new StringBuilder()
				.append("INSTANT_FLAG DELETED_FLAG MIN_REC_FLAG N_OWNED HEAP_NO          RECORD_TYPE REC_NEXT(OffSet|Pos)\n")
				.append("------------ ------------ ------------ ------- ------- -------------------- --------------------\n")
				.append(String.format("%12s ", extra.getInstantFlag()))
				.append(String.format("%12s ", extra.getDeletedFlag()))
				.append(String.format("%12s ", extra.getMinRecFlag()))
				.append(String.format("%7d ", extra.getRecordsOwned()))
				.append(String.format("%7d ", extra.getRecordHeapNo()))
				.append(String.format("%20s ", RecordExtra.getRecordType( extra.getRecordStatus())))
				.append(String.format("%20s", extra.getNextRecordOffset() + " | " + extra.getNextRecordPos()))
				.append("\n");
		return buff.toString();
	}
	
	public static String dumpSystemRecords(SystemRecords systemRecords) {
		RecordExtra infimumExtra = systemRecords.getInfimumExtra();
		RecordExtra supremumExtra = systemRecords.getSupremumExtra();
		StringBuilder buff = new StringBuilder();
		buff.append("INSTANT_FLAG DELETED_FLAG MIN_REC_FLAG N_OWNED HEAP_NO          RECORD_TYPE REC_NEXT(OffSet|Pos)     Content\n")
			.append("------------ ------------ ------------ ------- ------- -------------------- -------------------- -----------\n")
			.append(String.format("%12s ", infimumExtra.getInstantFlag()))
			.append(String.format("%12s ", infimumExtra.getDeletedFlag()))
			.append(String.format("%12s ", infimumExtra.getMinRecFlag()))
			.append(String.format("%7d ", infimumExtra.getRecordsOwned()))
			.append(String.format("%7d ", infimumExtra.getRecordHeapNo()))
			.append(String.format("%20s ", RecordExtra.getRecordType( infimumExtra.getRecordStatus())))
			.append(String.format("%20s ", infimumExtra.getNextRecordOffset() + " | " + infimumExtra.getNextRecordPos()))
			.append(String.format("%11s", new String(systemRecords.getInfimumContentRaw())))
			.append("\n")
			.append(String.format("%12s ", supremumExtra.getInstantFlag()))
			.append(String.format("%12s ", supremumExtra.getDeletedFlag()))
			.append(String.format("%12s ", supremumExtra.getMinRecFlag()))
			.append(String.format("%7d ", supremumExtra.getRecordsOwned()))
			.append(String.format("%7d ", supremumExtra.getRecordHeapNo()))
			.append(String.format("%20s ", RecordExtra.getRecordType( supremumExtra.getRecordStatus())))
			.append(String.format("%20s ", supremumExtra.getNextRecordOffset() + " | " + supremumExtra.getNextRecordPos()))
			.append(String.format("%11s", new String(systemRecords.getSupremumContentRaw())))
			.append("\n");
		return buff.toString();
	}

	public static String dumpClusteredKeyLeafRecords(List<ClusteredKeyLeafRecord> userRecords, boolean withDetails) {
		StringBuilder buff = new StringBuilder();
		if (Objects.isNull(userRecords) || userRecords.size() < 1) {
			buff.append("Empty or Null set");
			return buff.toString();
		}
		int len = getMaxColumnLength(userRecords.get(0), withDetails);
		String format = "%" + len + "s";

		for (ClusteredKeyLeafRecord userRecord : userRecords) {
			byte[] varLenRaw = userRecord.getVariableFieldLengthsRaw();
			buff.append("************************************************").append(NL);
			if(withDetails) {
				BitSet nullBitmap = userRecord.getNullBitmap();
				buff.append("VARIABLE-LENGTH: ").append(Objects.isNull(varLenRaw) ? "" : toHexString(varLenRaw)).append(NL)
					.append("    NULL-BITMAP: ").append(Objects.isNull(nullBitmap) ? "" : toBinaryString(nullBitmap, 0, 7)).append(NL)
					.append("    EXTRA-BYTES: 0x").append(toHexString(userRecord.getRecordExtraRaw())).append(NL)
					.append("    INSTANT_FLAG | DELETED_FLAG | MIN_REC_FLAG | N_OWNED | HEAP_NO |          RECORD_TYPE | REC_NEXT ").append(NL)
					.append("    -------------+--------------+--------------+---------+---------+----------------------+----------").append(NL)
					.append("    ")
					.append(String.format("%12s", userRecord.getInstantFlag())).append(" | ")
					.append(String.format("%12s", userRecord.getDeletedFlag())).append(" | ")
					.append(String.format("%12s", userRecord.getMinRecFlag())).append(" | ")
					.append(String.format("%7s", userRecord.getRecordsOwned())).append(" | ")
					.append(String.format("%7s", userRecord.getRecordHeapNo())).append(" | ")
					.append(String.format("%20s", RecordExtra.getRecordType(userRecord.getRecordStatus()))).append(" | ")
					.append(String.format("%8s", userRecord.getNextRecordOffset())).append(NL).append(NL);
			}
			final List<RecordField> fields = userRecord.getRecordFields();
			for (RecordField field : fields) {
				if (field.isInternal() && !withDetails) {
					continue;
				}
				buff.append(String.format(format, field.getName())).append(": ").append(field.getContent())
					// .append(" ("+(Objects.isNull(field.getConetentRaw())?"null":ParserHelper.toHexString(field.getConetentRaw()))).append(")")
					.append(NL);
			}
			buff.append(NL);
		}
		return buff.toString();
	}

	public static String dumpClusteredKeyNonLeafRecords(List<ClusteredKeyNonLeafRecord> userRecords) {
		StringBuilder buff = new StringBuilder();
		if (Objects.isNull(userRecords) || userRecords.size() < 1) {
			buff.append("Empty or Null set");
			return buff.toString();
		}
		for (ClusteredKeyNonLeafRecord record : userRecords) {
			long childPageNo = record.getChildPageNumber();
			List<RecordField> minKeyOnChildPage = record.getMinClusterKeyOnChild();
			StringBuilder keyContent = new StringBuilder();
			for(RecordField field: minKeyOnChildPage) {
				keyContent.append("  ").append(field.getName()).append(" = ").append(field.getContent()).append(NL);
			}
			buff.append("Child Page#: ").append(childPageNo).append(NL)
				.append("Min. Key: ").append(NL)
				.append(keyContent).append(NL);
		}
		return buff.toString();
	}
	
	public static String dumpSecondaryKeyLeafRecords(List<SecondaryKeyLeafRecord> userRecords) {
		StringBuilder buff = new StringBuilder();
		if (Objects.isNull(userRecords) || userRecords.size() < 1) {
			buff.append("Empty or Null set");
			return buff.toString();
		}
		for (SecondaryKeyLeafRecord record : userRecords) {
			buff//.append("************************************************").append(NL)
				.append("Secondary Key Fields: ").append(NL);
			for (RecordField field : record.getSecondaryKeyFields()) {
				buff.append(field.getName()).append(":").append(field.getContent()).append(NL);
			}
			buff.append("Cluster Key Fields: ").append(NL);
			for (RecordField field : record.getClusterKeyFields()) {
				buff.append(field.getName()).append(":").append(field.getContent()).append(NL);
			}
		}
		return buff.toString();
	}
	
	public static String dumpSecondaryKeyNonLeafRecords(List<SecondaryKeyNonLeafRecord> userRecords) {
		StringBuilder buff = new StringBuilder();
		if (Objects.isNull(userRecords) || userRecords.size() < 1) {
			buff.append("Empty or Null set");
			return buff.toString();
		}
		for (SecondaryKeyNonLeafRecord record : userRecords) {
			buff//.append("************************************************").append(NL)
				.append("Child Page No.: ").append(record.getChildPageNumber()).append(NL)
				.append("Min. Secondary Key Fields: ").append(NL);
			for (RecordField field : record.getMinSecondaryKeyOnChild()) {
				buff.append(field.getName()).append(":").append(field.getContent()).append(NL);
			}
			buff.append("Min. Cluster Key Fields: ").append(NL);
			for (RecordField field : record.getMinClusterKeyOnChild()) {
				buff.append(field.getName()).append(":").append(field.getContent()).append(NL);
			}
		}
		return buff.toString();
	}
	
	
	public static String dumpIndexPage(IndexPage page, TableMeta tableMeta) {
		StringBuilder buff = new StringBuilder();
		FilHeader pgHealer = page.getFilHeader();
		IndexHeader idxHeader = page.getIndexHeader();
		FsegHeader fsHeader = page.getFsegHeader();
		SystemRecords systemRecords = page.getSystemRecords();
		
		buff.append("##########################  FIL Header ##########################").append(NL)
				.append(dumpFilHeader(pgHealer)).append(NL)
				.append("########################## INDEX Header ##########################").append(NL)
				.append(dumpIndexHeader(idxHeader)).append(NL)
				.append("########################## FSEG Header ##########################").append(NL)
				.append(dumpFsegHeader(fsHeader)).append(NL)
				.append("########################## System Records ##########################").append(NL)
				.append(dumpSystemRecords(systemRecords)).append(NL);
		buff.append("########################## User Records ##########################").append(NL);
		if (page instanceof ClusteredKeyLeafPage) {
			List<ClusteredKeyLeafRecord> userRecords = ((ClusteredKeyLeafPage) page).getUserRecords(tableMeta);
			buff.append(dumpClusteredKeyLeafRecords(userRecords, true)).append(NL);
		} else if (page instanceof ClusteredKeyNonLeafPage) {
			List<ClusteredKeyNonLeafRecord> userRecords = ((ClusteredKeyNonLeafPage) page).getUserRecords(tableMeta);
			buff.append(dumpClusteredKeyNonLeafRecords(userRecords)).append(NL);
		} else if (page instanceof SecondaryKeyLeafPage) {
			List<SecondaryKeyLeafRecord> userRecords = ((SecondaryKeyLeafPage) page).getUserRecords(tableMeta);
			buff.append(dumpSecondaryKeyLeafRecords(userRecords)).append(NL);
		} else if (page instanceof SecondaryKeyNonLeafPage) {
			List<SecondaryKeyNonLeafRecord> userRecords = ((SecondaryKeyNonLeafPage) page).getUserRecords(tableMeta);
			buff.append(dumpSecondaryKeyNonLeafRecords(userRecords)).append(NL);
		}
		return buff.toString();
	}
	
	

	
	private static int getMaxColumnLength(ClusteredKeyLeafRecord userRecord, boolean withInternal) {
		final List<RecordField> fields = userRecord.getRecordFields();
		int maxLen = 0;
		for (RecordField field : fields) {
			if (field.isInternal()) {
				if (!withInternal) {
					continue;
				}
			}
			maxLen = Math.max(maxLen, field.getName().length());
		}
		return maxLen;
	}
}
