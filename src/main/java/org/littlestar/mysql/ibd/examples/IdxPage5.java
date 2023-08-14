package org.littlestar.mysql.ibd.examples;

import java.util.List;

import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage;
import org.littlestar.mysql.ibd.page.Page;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage.ClusteredKeyLeafRecord;
import org.littlestar.mysql.ibd.page.IndexPage.RecordField;
import org.littlestar.mysql.ibd.parser.IbdFileParser;

public class IdxPage5 {
	public static void main(String[] args) throws Exception {
		String fileName = "D:\\Data\\mysql\\8.0.18\\data\\sakila\\film.ibd";
		try (IbdFileParser parser = new IbdFileParser(fileName)) {
			// 通过上文"InnoDB文件物理结构解析4"可知，page(14)包含film_id=(565, 668)之间的数据;
			Page page = parser.getPage(14);
			// System.out.println(ParserHelper.hexDump(page.getPageRaw()));
			ClusteredKeyLeafPage leafPage = new ClusteredKeyLeafPage(page.getPageRaw(), page.getPageSize());
			List<ClusteredKeyLeafRecord> garbageRecords = leafPage.getUserRecords(IdxPage3.getFilmTableMeta());
			StringBuilder buff = new StringBuilder();
			for (ClusteredKeyLeafRecord record : garbageRecords) {
				List<RecordField> fields = record.getRecordFields();
				buff.append("\n ==> Extra: deleted = ").append(record.getDeletedFlag()).append("; next offset = ")
						.append(record.getNextRecordOffset()).append(" <==\n");
				for (RecordField field : fields) {
					buff.append(String.format("%20s", field.getName())).append(": ").append(field.getContent()).append("\n");
				}
			}
			System.out.println(buff);
		}
	}
}
