package org.littlestar.mysql.ibd;

import static org.littlestar.mysql.ibd.parser.ColumnMeta.*;
import static org.littlestar.mysql.ibd.parser.ColumnType.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage.ClusteredKeyLeafRecord;
import org.littlestar.mysql.ibd.page.IndexPage.RecordField;
import org.littlestar.mysql.ibd.page.Page;
import org.littlestar.mysql.ibd.parser.ColumnMeta;
import org.littlestar.mysql.ibd.parser.IbdFileParser;
import org.littlestar.mysql.ibd.parser.TableMeta;

class RowIdTest {

	TableMeta getTableMeta() {
		ColumnMeta rowId = ColumnMeta.newRowIdColumnMeta(1);
		ColumnMeta trxId = newTrxIdColumnMeta(2);
		ColumnMeta rollPtr = newRollPtrColumnMeta(3);
		ColumnMeta col1 = newFixLengthColumnMeta(INT, 4, "col1", true);
		ColumnMeta col2 = newDecimalColumnMeta(5, "col2", true, 3, 1);
		TableMeta tableMeta = new TableMeta()
				.addColumn(rowId)
				.addColumn(trxId)
				.addColumn(rollPtr)
				.addColumn(col1)
				.addColumn(col2)
				.setClusterKey(1, rowId);
		return tableMeta;
	}

	@Test
	void testRowId() throws IOException, Exception {
		String fileName = "src/test/cases/ibd/8.0.18/row_id.ibd";
		try (IbdFileParser parser = new IbdFileParser(fileName)) {
			Page page = parser.getPage(4);
			ClusteredKeyLeafPage indexPage = new ClusteredKeyLeafPage(page.getPageRaw(), page.getPageSize());
			List<ClusteredKeyLeafRecord> records = indexPage.getUserRecords(getTableMeta());
			for (ClusteredKeyLeafRecord record : records) {
				List<RecordField> fields = record.getRecordFields();
				RecordField rowIdField = fields.get(0);
				RecordField col1Field = fields.get(3);
				RecordField col2Field = fields.get(4);
				byte[] rowId = rowIdField.getConetentRaw();
				Object col1 = col1Field.getContent();
				Object col2 = col2Field.getContent();
				//System.out.println(ParserHelper.toHexString(rowIdField.getConetentRaw()));
				//System.out.println(col1);
				//System.out.println(col2);
				if (Objects.equals(rowId, new byte[] { 0x00, 0x00, 0x00, (byte) 0xf2, (byte) 0xa1, 0x06 })) {
					assertEquals(1, col1);
					assertEquals(1.1D, col2);
				} else if (Objects.equals(rowId, new byte[] { 0x00, 0x00, 0x00, (byte) 0xf2, (byte) 0xa1, 0x07 })) {
					assertNull(col1);
					assertEquals(2.3D, col2);
				} else if (Objects.equals(rowId, new byte[] { 0x00, 0x00, 0x00, (byte) 0xf2, (byte) 0xa1, 0x08 })) {
					assertEquals(3, col1);
					assertNull(col2);
				}
			}
		}
	}
}
