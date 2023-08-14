package org.littlestar.mysql.ibd.examples;


import org.littlestar.mysql.ibd.page.SecondaryKeyLeafPage;
import org.littlestar.mysql.ibd.page.SecondaryKeyLeafPage.SecondaryKeyLeafRecord;

import java.util.List;

import org.littlestar.mysql.ibd.page.IndexPage;
import org.littlestar.mysql.ibd.page.IndexPage.RecordField;
import org.littlestar.mysql.ibd.parser.IbdFileParser;
import org.littlestar.mysql.ibd.parser.TableMeta;

public class IdxPage6 {
	public static void main(String[] args) throws Exception {
		String fileName = "D:\\Data\\mysql\\8.0.18\\data\\sakila\\film.ibd";
		try (IbdFileParser parser = new IbdFileParser(fileName)) {
			IndexPage page = (IndexPage) parser.getPage(16);
			long indexId = page.getIndexHeader().getIndexId().longValueExact();
			TableMeta tableMeta = IdxPage3.getFilmTableMeta();
			//// 定义索引的元数据(索引包含哪些列)
			tableMeta.setSecondaryKey(indexId, 1, "title");
			SecondaryKeyLeafPage leafPage = new SecondaryKeyLeafPage(page.getPageRaw(), page.getPageSize());
			List<SecondaryKeyLeafRecord> records = leafPage.getUserRecords(tableMeta);
			StringBuilder buff = new StringBuilder();
			buff.append("Secondary Key Fields             Cluster Key Fields      \n")
				.append("-------------------------------- ------------------------\n");
			for (SecondaryKeyLeafRecord record : records) {
				List<RecordField> skFields = record.getSecondaryKeyFields();
				List<RecordField> ckFields = record.getClusterKeyFields();
				
				StringBuilder skValue = new StringBuilder();
				StringBuilder ckValue = new StringBuilder();
				for(RecordField field: skFields) {
					skValue.append(field.getName()).append(" = ").append(field.getContent()).append(" ");
				}
				buff.append(String.format("%-32s ", skValue));
				for(RecordField field: ckFields) {
					ckValue.append(field.getName()).append(" = ").append(field.getContent()).append(" ");
				}
				buff.append(String.format("%-24s\n", ckValue));
			}
			System.out.println(buff);
		}
	}
}
