package org.littlestar.mysql.ibd.examples;

import java.math.BigInteger;
import java.util.List;

import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage.ClusteredKeyLeafRecord;
import org.littlestar.mysql.ibd.page.ClusteredKeyNonLeafPage;
import org.littlestar.mysql.ibd.page.ClusteredKeyNonLeafPage.ClusteredKeyNonLeafRecord;
import org.littlestar.mysql.ibd.page.IndexPage.RecordField;
import org.littlestar.mysql.ibd.page.Page;
import org.littlestar.mysql.ibd.parser.IbdFileParser;

public class IdxPage4 {
	public static void main(String[] args) throws Exception {
		String fileName = "D:\\Data\\mysql\\8.0.18\\data\\sakila\\film.ibd";
		try (IbdFileParser parser = new IbdFileParser(fileName)) {
			Page page = parser.getPage(4);
			ClusteredKeyNonLeafPage rootPage = new ClusteredKeyNonLeafPage(page.getPageRaw(), page.getPageSize());
			List<ClusteredKeyNonLeafRecord> rootRecords = rootPage.getUserRecords(IdxPage3.getFilmTableMeta());
			StringBuilder buff = new StringBuilder();
			buff.append("Cluster Key Min. Key  Child Page Number \n")
				.append("--------------------  ----------------- \n");
			for(ClusteredKeyNonLeafRecord record: rootRecords) {
				long childPage = record.getChildPageNumber(); 
				List<RecordField> minKeys = record.getMinClusterKeyOnChild();
				String minKeyValue = "";
				for (RecordField minKey : minKeys) {
					minKeyValue += (minKey.getName() + " = " + minKey.getContent() + " ");
				}
				buff.append(String.format("%21s ", minKeyValue))
					.append(String.format("%17d", childPage))
					.append("\n");
			}
			System.out.println(buff);
			
			////
			page = parser.getPage(14);
			ClusteredKeyLeafPage leafPage = new ClusteredKeyLeafPage(page.getPageRaw(), page.getPageSize());
			List<ClusteredKeyLeafRecord> leafRecords = leafPage.getUserRecords(IdxPage3.getFilmTableMeta());
			for(ClusteredKeyLeafRecord record: leafRecords) {
				List<RecordField> fields = record.getRecordFields(); //有序列表, 按字段顺序，从0开始
				RecordField filmIdField = fields.get(0);
				long filmId = ((BigInteger)filmIdField.getContent()).longValueExact();
				if(filmId == 666L) {
					for(RecordField field: fields) {
						System.out.println(String.format("%20s", field.getName())+": "+ field.getContent());
					}
				}
			}
		}
	}
}
