package org.littlestar.mysql.ibd.examples;

import static org.littlestar.mysql.ibd.parser.ColumnMeta.newColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.newDecimalColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.newEnumColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.newFixLengthColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.newRollPtrColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.newSetColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.newTrxIdColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnType.TEXT;
import static org.littlestar.mysql.ibd.parser.ColumnType.TIMESTAMP;
import static org.littlestar.mysql.ibd.parser.ColumnType.UNSIGNED_SMALLINT;
import static org.littlestar.mysql.ibd.parser.ColumnType.UNSIGNED_TINYINT;
import static org.littlestar.mysql.ibd.parser.ColumnType.VARCHAR;
import static org.littlestar.mysql.ibd.parser.ColumnType.YEAR;

import java.util.List;
import java.util.Map;

import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage;
import org.littlestar.mysql.ibd.page.FilHeader;
import org.littlestar.mysql.ibd.page.IndexPage;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage.ClusteredKeyLeafRecord;
import org.littlestar.mysql.ibd.page.IndexPage.IndexHeader;
import org.littlestar.mysql.ibd.page.IndexPage.RecordField;
import org.littlestar.mysql.ibd.parser.ColumnMeta;
import org.littlestar.mysql.ibd.parser.IbdFileParser;
import org.littlestar.mysql.ibd.parser.TableMeta;

public class IdxPage3 {
	public static TableMeta getFilmTableMeta() {
		ColumnMeta filmId  = newFixLengthColumnMeta(UNSIGNED_SMALLINT, 1, "film_id", false);
		ColumnMeta trxId   = newTrxIdColumnMeta(2);
		ColumnMeta rollPtr = newRollPtrColumnMeta(3);
		ColumnMeta title   = newColumnMeta(VARCHAR, 4, "title", 512, false, true);
		ColumnMeta description = newColumnMeta(TEXT, 5, "description", 65535, true, true);
		ColumnMeta releaseYear = newFixLengthColumnMeta(YEAR, 6, "release_year", true);
		ColumnMeta languageId = newFixLengthColumnMeta(UNSIGNED_TINYINT, 7, "language_id", false);
		ColumnMeta originalLanguageId = newFixLengthColumnMeta(UNSIGNED_TINYINT, 8, "original_language_id", true);
		ColumnMeta rentalDuration = newFixLengthColumnMeta(UNSIGNED_TINYINT, 9, "rental_duration", false);
		ColumnMeta rentalRate = newDecimalColumnMeta(10, "rental_rate", false, 4, 2);
		ColumnMeta length = newFixLengthColumnMeta(UNSIGNED_SMALLINT, 11, "length", true);
		ColumnMeta replacementCost = newDecimalColumnMeta(12, "replacement_cost", false, 5, 2);
		ColumnMeta rating = newEnumColumnMeta(13, "rating", true, "G","PG","PG-13","R","NC-17");
		ColumnMeta specialFeatures = newSetColumnMeta(14, "special_features", true, "Trailers","Commentaries","Deleted Scenes","Behind the Scenes");
		ColumnMeta lastUpdate = newFixLengthColumnMeta(TIMESTAMP, 15, "last_update", false);
		TableMeta tableMeta = new TableMeta()
				.addColumn(filmId)
				.addColumn(trxId)
				.addColumn(rollPtr)
				.addColumn(title)
				.addColumn(description)
				.addColumn(releaseYear)
				.addColumn(languageId)
				.addColumn(originalLanguageId)
				.addColumn(rentalDuration)
				.addColumn(rentalRate)
				.addColumn(length)
				.addColumn(replacementCost)
				.addColumn(rating)
				.addColumn(specialFeatures)
				.addColumn(lastUpdate)
				.setClusterKey(1, filmId);
		return tableMeta;
	}
	
	public static void main(String[] args) throws Exception {
		String fileName = "D:\\Data\\mysql\\8.0.18\\data\\sakila\\film.ibd";
		try (IbdFileParser parser = new IbdFileParser(fileName)) {
			Map<Integer, List<Long>> pageTypeMap = parser.getPageTypeMap();
			List<Long> indexPageNumbers = pageTypeMap.get(FilHeader.FIL_PAGE_INDEX);
			IndexPage pkRoot = (IndexPage) parser.getPage(4);
			long pkId = pkRoot.getIndexHeader().getIndexId().longValueExact();
			StringBuilder buff = new StringBuilder();
			int recCount = 0;
			for (long pageNumber : indexPageNumbers) {
				IndexPage indexPage = (IndexPage) parser.getPage(pageNumber);
				IndexHeader indexHeader = indexPage.getIndexHeader();
				int level = indexHeader.getPageLevel();
				long indexId = indexHeader.getIndexId().longValueExact();
				if (indexId == pkId && level == 0) {
					List<ClusteredKeyLeafRecord> userRecords = new ClusteredKeyLeafPage(indexPage.getPageRaw(),
							indexPage.getPageSize()).getUserRecords(getFilmTableMeta());
					for (ClusteredKeyLeafRecord userRecord : userRecords) {
						List<RecordField> fields = userRecord.getRecordFields();
						buff.append("\n*************************** ").append(++recCount).append(". row ***************************\n");
						for (RecordField field : fields) {
							buff.append(String.format("%20s", field.getName())).append(": ").append(field.getContent())
									.append("\n");
						}
					}
				}
			}
			System.out.println(buff);
		}
	}
}
