package org.littlestar.mysql.ibd.examples;

import java.io.IOException;
import java.util.List;
import org.littlestar.mysql.ibd.page.FilHeader;
import org.littlestar.mysql.ibd.page.IndexPage;
import org.littlestar.mysql.ibd.page.IndexPage.IndexHeader;
import org.littlestar.mysql.ibd.parser.IbdFileParser;

public class IdxPage1 {
	public static void main(String[] args) throws IOException, Exception {
		String fileName = "D:\\Data\\mysql\\8.0.18\\data\\sakila\\film.ibd";
		try (IbdFileParser parser = new IbdFileParser(fileName)) {
			List<Long> pageNums = parser.getPageTypeMap().get(FilHeader.FIL_PAGE_INDEX);
			StringBuilder buff = new StringBuilder();
			buff.append(" PAGE       PAGE_TYPE LEVEL INDEX_ID   PAGE_PREV   PAGE_NEXT\n")
				.append("----- --------------- ----- -------- ----------- -----------\n");
			for (long pageNum : pageNums) {
				IndexPage indexPage = (IndexPage) parser.getPage(pageNum);
				FilHeader filHeader = indexPage.getFilHeader();
				IndexHeader indexHeader = indexPage.getIndexHeader();
				buff.append(String.format("%5d ", pageNum))
					.append(String.format("%15s ", filHeader.getPageTypeName()))
					.append(String.format("%5d ", indexHeader.getPageLevel()))
					.append(String.format("%8d ", indexHeader.getIndexId()))
					.append(String.format("%11d ", filHeader.getPreviousPage()))
					.append(String.format("%11d ", filHeader.getNextPage()))
					.append("\n");
			}
			System.out.println(buff);
		}
	}
}
