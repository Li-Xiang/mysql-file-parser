package org.littlestar.mysql.ibd.examples;

import java.io.IOException;
import java.util.Arrays;

import org.littlestar.mysql.ibd.page.IndexPage;
import org.littlestar.mysql.ibd.page.IndexPage.SystemRecords;
import org.littlestar.mysql.ibd.page.RecordExtra;
import org.littlestar.mysql.ibd.parser.IbdFileParser;

public class IdxPage2 {
	// SUPREMUM_REC_NEXT记录的结束位置, 根据System_Records结构图, 固定为112;
	public static final int SUPREMUM_REC_NEXT_END_POS = 112;
	// INFIMUM_REC_NEXT记录的结束位置, 根据System_Records结构图, 固定为99;
	public static final int INFIMUM_REC_NEXT_END_POS = 99;
	public static void main(String[] args) throws IOException, Exception {
		String fileName = "D:\\Data\\mysql\\8.0.18\\data\\sakila\\film.ibd";
		try (IbdFileParser parser = new IbdFileParser(fileName)) {
			IndexPage page = (IndexPage) parser.getPage(4);
			byte[] pageRaw = page.getPageRaw(); // 获取page(4)的页的数据(字节)
			SystemRecords systemRecords = page.getSystemRecords();
			
			RecordExtra infimumExtra = systemRecords.getInfimumExtra();   
			RecordExtra supremumExtra = systemRecords.getSupremumExtra();
			StringBuilder buff = new StringBuilder();
			buff.append("INSTANT_FLAG DELETED_FLAG MIN_REC_FLAG N_OWNED HEAP_NO          RECORD_TYPE REC_NEXT(OffSet|Pos)\n")
				.append("------------ ------------ ------------ ------- ------- -------------------- --------------------\n");
			// infimum next是一个有符号的整数(Int16), 如果你插入的数据是无序的，偏移有可能是个负数。
			// 这个相对偏移, 也就是INFIMUM_REC_NEXT结束的位置 + INFIMUM_REC_NEXT_END_POS
			int infimumRecOffset = infimumExtra.getNextRecordOffset();
			int infimumRecNextPos = infimumRecOffset + INFIMUM_REC_NEXT_END_POS;
			buff.append(dumpRecordExtra(infimumExtra));
			
			int currentPos = infimumRecNextPos;
			while (currentPos > SUPREMUM_REC_NEXT_END_POS) {
				int extraEndPos = currentPos;
				int extraStartPos = currentPos - IndexPage.REC_N_NEW_EXTRA_BYTES;//  5 bytes for DYNAMIC
				byte[] extraRaw = Arrays.copyOfRange(pageRaw, extraStartPos, extraEndPos);
				RecordExtra extra = new RecordExtra(extraRaw);
				int nextOffset = extra.getNextRecordOffset();
				int nextPos = nextOffset + extraEndPos;
				extra.setNextRecordPos(nextPos);
				buff.append(dumpRecordExtra(extra));
				if (nextPos == SUPREMUM_REC_NEXT_END_POS) {
					break;
				}
				currentPos = nextPos;
			}
			buff.append(dumpRecordExtra(supremumExtra));
			System.out.println(buff);
		}
	}
	
	public static String dumpRecordExtra(RecordExtra extra) {
		StringBuilder buff = new StringBuilder()
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
}
