package org.littlestar.mysql.ibd.examples;


import java.util.List;

import org.littlestar.mysql.ibd.page.IndexPage;
import org.littlestar.mysql.ibd.page.IndexPage.RecordField;
import org.littlestar.mysql.ibd.page.SecondaryKeyNonLeafPage;
import org.littlestar.mysql.ibd.page.SecondaryKeyNonLeafPage.SecondaryKeyNonLeafRecord;
import org.littlestar.mysql.ibd.parser.IbdFileParser;
import org.littlestar.mysql.ibd.parser.TableMeta;

public class IdxPage7 {
	public static void main(String[] args) throws Exception {
		String fileName = "D:\\Data\\mysql\\8.0.18\\data\\sakila\\film.ibd";
		try (IbdFileParser parser = new IbdFileParser(fileName)) {
			IndexPage page = (IndexPage) parser.getPage(5);
			long indexId = page.getIndexHeader().getIndexId().longValueExact();
			TableMeta tableMeta = IdxPage3.getFilmTableMeta();
			//// 定义索引的元数据(索引包含哪些列)
			tableMeta.setSecondaryKey(indexId, 1, "title");
			SecondaryKeyNonLeafPage rootPage = new SecondaryKeyNonLeafPage(page.getPageRaw(), page.getPageSize());
			List<SecondaryKeyNonLeafRecord> records = rootPage.getUserRecords(tableMeta);
			StringBuilder buff = new StringBuilder();
			buff.append("Secondary Key Min. Key on Child Page Cluster Key Min. Key on Child Page  Child Page Number \n")
				.append("------------------------------------ ----------------------------------- ------------------\n");
			for(SecondaryKeyNonLeafRecord record : records) {
				List<RecordField> minSkFields = record.getMinSecondaryKeyOnChild();
				List<RecordField> minCkFields = record.getMinClusterKeyOnChild();
				long childPageNo = record.getChildPageNumber();
				StringBuilder skValue = new StringBuilder();
				StringBuilder ckValue = new StringBuilder();
				for(RecordField field: minSkFields) {
					skValue.append(field.getName()).append(" = ").append(field.getContent()).append(" ");
				}
				buff.append(String.format("%-36s ", skValue));
				for(RecordField field: minCkFields) {
					ckValue.append(field.getName()).append(" = ").append(field.getContent()).append(" ");
				}
				buff.append(String.format("%-35s ", ckValue));
				buff.append(String.format("%-15d\n", childPageNo));
			}
			System.out.println(buff);
		}
	}
}
