package org.littlestar.mysql.ibd.examples;

import java.io.IOException;

import org.littlestar.mysql.ibd.page.FilHeader;
import org.littlestar.mysql.ibd.page.Page;
import org.littlestar.mysql.ibd.parser.IbdFileParser;

import static org.littlestar.mysql.common.ParserHelper.*;

public class IbdFile1 {
	public static void main(String[] args) throws IOException, Exception {
		String fileName = "D:\\Data\\mysql\\8.0.18\\data\\sakila\\film.ibd";
		try (IbdFileParser parser = new IbdFileParser(fileName)) {
			StringBuilder buff = new StringBuilder();
			buff.append("PAGE   CHECKSUM PAGE_OFFSET  PAGE_PREV  PAGE_NEXT           PAGE_LSN              PAGE_TYPE           FLUSH_LSN SPACE_ID\n")
			    .append("---- ---------- ----------- ---------- ---------- ------------------ ----------------------- ------------------ --------\n");
			for (long i = 0; i < parser.getPageCount(); i++) {
				Page page = parser.getPage(i);
				FilHeader filHeader = page.getFilHeader();
				buff.append(String.format("%4d ", i))
					.append(String.format("0x%4s ", toHexString(filHeader.getCheckSumRaw())))
					.append(String.format("%11d ", filHeader.getPageOffset()))
					.append(String.format("%10d ", filHeader.getPreviousPage()))//.append(" "+toHexString(filHeader.getPreviousPageRaw()))
					.append(String.format("%10d ", filHeader.getNextPage()))
					.append(String.format("0x%16s ", toHexString(filHeader.getPageLSNRaw())))
					.append(String.format("%23s ", filHeader.getPageTypeName()))
					.append(String.format("0x%16s ", toHexString(filHeader.getFlushLSNRaw())))
					.append(String.format("%8d", filHeader.getSpaceId()))
					.append("\n");
			}
			System.out.println(buff);
		}
	}
}
