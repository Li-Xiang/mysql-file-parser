package org.littlestar.mysql.ibd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.*;
import static org.littlestar.mysql.ibd.parser.ColumnType.*;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.littlestar.mysql.ibd.page.Page;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage;
import org.littlestar.mysql.ibd.page.IndexPage.RecordField;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage.ClusteredKeyLeafRecord;
import org.littlestar.mysql.ibd.parser.ColumnMeta;
import org.littlestar.mysql.ibd.parser.IbdFileParser;
import org.littlestar.mysql.ibd.parser.TableMeta;

class DataTypeTest {
	
	/**
	 * 
	 * root@localhost [testcase]> select * from integer_types;
	 * +-----------+-----------+------------+------------+-------------+-------------+-------------+------------+----------------------+----------------------+
	 * | tinyint_s | tinyint_u | smallint_s | smallint_u | mediumint_s | mediumint_u | int_s       | int_u      | bigint_s             | bigint_u             |
	 * +-----------+-----------+------------+------------+-------------+-------------+-------------+------------+----------------------+----------------------+
	 * |      -128 |         0 |     -32768 |          0 |    -8388608 |           0 | -2147483648 |          0 | -9223372036854775808 |                    0 |
	 * |       -97 |        97 |      -1997 |       1997 |       -1997 |        1997 |       -1997 |       1997 |                -1997 |                 1997 |
	 * |       -66 |        66 |      -6666 |       6666 |     -666666 |      666666 |  -666666666 |  666666666 |  -666666666666666666 |   666666666666666666 |
	 * |        -1 |         1 |         -1 |          1 |          -1 |           1 |          -1 |          1 |                   -1 |                    1 |
	 * |         0 |         0 |          0 |          0 |           0 |           0 |           0 |          0 |                    0 |                    0 |
	 * |        97 |        97 |       1997 |       1997 |        1997 |        1997 |        1997 |       1997 |                 1997 |                 1997 |
	 * |       127 |       255 |      32767 |      65535 |     8388607 |    16777215 |  2147483647 | 4294967295 |  9223372036854775807 | 18446744073709551615 |
	 * +-----------+-----------+------------+------------+-------------+-------------+-------------+------------+----------------------+----------------------+
	 * 7 rows in set (0.00 sec)
	 * 
	 */
	
	final String itsFieldName1 = "tinyint_s";
	final String itsFieldName2 = "tinyint_u";
	final String itsFieldName3 = "smallint_s";
	final String itsFieldName4 = "smallint_u";
	final String itsFieldName5 = "mediumint_s";
	final String itsFieldName6 = "mediumint_u";
	final String itsFieldName7 = "int_s";
	final String itsFieldName8 = "int_u";
	final String itsFieldName9 = "bigint_s";
	final String itsFieldName10 = "bigint_u";
	
	TableMeta getIntegerTypesTableMeta() {
		ColumnMeta pk = newFixLengthColumnMeta(TINYINT, 1, itsFieldName1, false);
		TableMeta tableMeta = new TableMeta()
				.addColumn(pk)
				.addColumn(newTrxIdColumnMeta(2))
				.addColumn(newRollPtrColumnMeta(3))
				.addColumn(newFixLengthColumnMeta(UNSIGNED_TINYINT  , 4, itsFieldName2, true))
				.addColumn(newFixLengthColumnMeta(SMALLINT          , 5, itsFieldName3, true))
				.addColumn(newFixLengthColumnMeta(UNSIGNED_SMALLINT , 6, itsFieldName4, true))
				.addColumn(newFixLengthColumnMeta(MEDIUMINT         , 7, itsFieldName5, true))
				.addColumn(newFixLengthColumnMeta(UNSIGNED_MEDIUMINT, 8, itsFieldName6, true))
				.addColumn(newFixLengthColumnMeta(INT            , 9, itsFieldName7  , true))
				.addColumn(newFixLengthColumnMeta(UNSIGNED_INT   , 10, itsFieldName8 , true))
				.addColumn(newFixLengthColumnMeta(BIGINT         , 11, itsFieldName9 , true))
				.addColumn(newFixLengthColumnMeta(UNSIGNED_BIGINT, 12, itsFieldName10, true))
				.setClusterKey(1, pk);
		return tableMeta;
	}
	
	@Test
	void testIntegerTypes() throws Exception {
		TableMeta tableMeta = getIntegerTypesTableMeta();
		String fileName = "src/test/cases/ibd/8.0.18/integer_types.ibd";
		try (IbdFileParser ibdFileParser = new IbdFileParser(fileName)) {
			Page page = ibdFileParser.getPage(4);
			ClusteredKeyLeafPage indexPage = new ClusteredKeyLeafPage(page.getPageRaw(), page.getPageSize());
			List<ClusteredKeyLeafRecord> userRecords = indexPage.getUserRecords(tableMeta);
			//ParseFilePageIndexHelper.printUserRecords(userRecords);
			//ParseFilePageIndexHelper.printSystemRecord(indexPage);
			assertEquals(userRecords.size(), 7);
			for (ClusteredKeyLeafRecord userRecord : userRecords) {
				List<RecordField> fields = userRecord.getRecordFields();
				int fieldSize = fields.size();
				assertEquals(fieldSize, tableMeta.getColumnCount());
				// Collections.sort(fields);
				RecordField pkField = fields.get(0);
				// 1 db_trx_id
				// 2 db_roll_ptr
				RecordField tinyintU = fields.get(3);
				RecordField smallintS = fields.get(4);
				RecordField smallintU = fields.get(5);
				RecordField mediumintS = fields.get(6);
				RecordField mediumintU = fields.get(7);
				RecordField intS = fields.get(8);
				RecordField intU = fields.get(9);
				RecordField bigintS = fields.get(10);
				RecordField bigintU = fields.get(11);

				assertEquals(itsFieldName1, pkField.getName());
				assertEquals(itsFieldName2, tinyintU.getName());
				assertEquals(itsFieldName3, smallintS.getName());
				assertEquals(itsFieldName4, smallintU.getName());
				assertEquals(itsFieldName5, mediumintS.getName());
				assertEquals(itsFieldName6, mediumintU.getName());
				assertEquals(itsFieldName7, intS.getName());
				assertEquals(itsFieldName8, intU.getName());
				assertEquals(itsFieldName9, bigintS.getName());
				assertEquals(itsFieldName10, bigintU.getName());

				int valuePk = ((BigInteger) pkField.getContent()).intValue();
				switch (valuePk) {
				case -128:
					assertEquals(new BigInteger("0"), tinyintU.getContent());
					assertEquals(new BigInteger("-32768"), smallintS.getContent());
					assertEquals(new BigInteger("0"), smallintU.getContent());
					assertEquals(new BigInteger("-8388608"), mediumintS.getContent());
					assertEquals(new BigInteger("0"), mediumintU.getContent());
					assertEquals(new BigInteger("-2147483648"), intS.getContent());
					assertEquals(new BigInteger("0"), intU.getContent());
					assertEquals(new BigInteger("-9223372036854775808"), bigintS.getContent());
					assertEquals(new BigInteger("0"), bigintU.getContent());
					break;
				case -97:
					assertEquals(new BigInteger("97"), tinyintU.getContent());
					assertEquals(new BigInteger("-1997"), smallintS.getContent());
					assertEquals(new BigInteger("1997"), smallintU.getContent());
					assertEquals(new BigInteger("-1997"), mediumintS.getContent());
					assertEquals(new BigInteger("1997"), mediumintU.getContent());
					assertEquals(new BigInteger("-1997"), intS.getContent());
					assertEquals(new BigInteger("1997"), intU.getContent());
					assertEquals(new BigInteger("-1997"), bigintS.getContent());
					assertEquals(new BigInteger("1997"), bigintU.getContent());
					break;
				case -66:
					assertEquals(new BigInteger("66"), tinyintU.getContent());
					assertEquals(new BigInteger("-6666"), smallintS.getContent());
					assertEquals(new BigInteger("6666"), smallintU.getContent());
					assertEquals(new BigInteger("-666666"), mediumintS.getContent());
					assertEquals(new BigInteger("666666"), mediumintU.getContent());
					assertEquals(new BigInteger("-666666666"), intS.getContent());
					assertEquals(new BigInteger("666666666"), intU.getContent());
					assertEquals(new BigInteger("-666666666666666666"), bigintS.getContent());
					assertEquals(new BigInteger("666666666666666666"), bigintU.getContent());
					break;
				case -1:
					assertEquals(new BigInteger("1"), tinyintU.getContent());
					assertEquals(new BigInteger("-1"), smallintS.getContent());
					assertEquals(new BigInteger("1"), smallintU.getContent());
					assertEquals(new BigInteger("-1"), mediumintS.getContent());
					assertEquals(new BigInteger("1"), mediumintU.getContent());
					assertEquals(new BigInteger("-1"), intS.getContent());
					assertEquals(new BigInteger("1"), intU.getContent());
					assertEquals(new BigInteger("-1"), bigintS.getContent());
					assertEquals(new BigInteger("1"), bigintU.getContent());
					break;
				case 0:
					assertEquals(new BigInteger("0"), tinyintU.getContent());
					assertEquals(new BigInteger("0"), smallintS.getContent());
					assertEquals(new BigInteger("0"), smallintU.getContent());
					assertEquals(new BigInteger("0"), mediumintS.getContent());
					assertEquals(new BigInteger("0"), mediumintU.getContent());
					assertEquals(new BigInteger("0"), intS.getContent());
					assertEquals(new BigInteger("0"), intU.getContent());
					assertEquals(new BigInteger("0"), bigintS.getContent());
					assertEquals(new BigInteger("0"), bigintU.getContent());
					break;
				case 97:
					assertEquals(new BigInteger("97"), tinyintU.getContent());
					assertEquals(new BigInteger("1997"), smallintS.getContent());
					assertEquals(new BigInteger("1997"), smallintU.getContent());
					assertEquals(new BigInteger("1997"), mediumintS.getContent());
					assertEquals(new BigInteger("1997"), mediumintU.getContent());
					assertEquals(new BigInteger("1997"), intS.getContent());
					assertEquals(new BigInteger("1997"), intU.getContent());
					assertEquals(new BigInteger("1997"), bigintS.getContent());
					assertEquals(new BigInteger("1997"), bigintU.getContent());
					break;
				case 127:
					assertEquals(new BigInteger("255"), tinyintU.getContent());
					assertEquals(new BigInteger("32767"), smallintS.getContent());
					assertEquals(new BigInteger("65535"), smallintU.getContent());
					assertEquals(new BigInteger("8388607"), mediumintS.getContent());
					assertEquals(new BigInteger("16777215"), mediumintU.getContent());
					assertEquals(new BigInteger("2147483647"), intS.getContent());
					assertEquals(new BigInteger("4294967295"), intU.getContent());
					assertEquals(new BigInteger("9223372036854775807"), bigintS.getContent());
					assertEquals(new BigInteger("18446744073709551615"), bigintU.getContent());
					break;
				default:
					throw new Exception("Unknown PK content in table integer_types");
				}
			}
		}
	}
	
	TableMeta getFloatTypeTableMeta() {
		ColumnMeta col1 = ColumnMeta.newFixLengthColumnMeta(FLOAT, 1, "col1", false);
		ColumnMeta col2 = ColumnMeta.newFixLengthColumnMeta(DOUBLE, 4, "col2", false);
		TableMeta tableMeta = new TableMeta()
				.addColumn(col1)
				.addColumn(ColumnMeta.newTrxIdColumnMeta(2))
				.addColumn(ColumnMeta.newRollPtrColumnMeta(3))
				.addColumn(col2)
				.setClusterKey(1, col1);
		return tableMeta;
	}
	
	/**
	 * root@localhost [testcase]> select * from float_type;
	 * +----------+-------------------+
	 * | col1     | col2              |
	 * +----------+-------------------+
	 * | -123.456 | -123456789.123456 |
	 * |      -10 |               -10 |
	 * |       10 |                10 |
	 * |  123.456 |  123456789.123456 |
	 * +----------+-------------------+
	 * 4 rows in set (0.01 sec)
	 */
	
	@Test
	void testFloatType() throws IOException, Exception {
		TableMeta tableMeta = getFloatTypeTableMeta();
		String fileName = "src/test/cases/ibd/8.0.18/float_type.ibd";
		try (IbdFileParser ibdFileParser = new IbdFileParser(fileName)) {
			Page page = ibdFileParser.getPage(4);
			ClusteredKeyLeafPage indexPage = new ClusteredKeyLeafPage(page.getPageRaw(), page.getPageSize());
			List<ClusteredKeyLeafRecord> userRecords = indexPage.getUserRecords(tableMeta);
			//System.out.println(dumpClusteredKeyLeafRecords(userRecords, false));
			int count = 0;
			for (ClusteredKeyLeafRecord record : userRecords) {
				List<RecordField> fields = record.getRecordFields();
				RecordField col1 = fields.get(0);
				RecordField col2 = fields.get(3);
				float col1Content = (float)col1.getContent();
				double col2Content = (double) col2.getContent();
				if (col1Content == -123.456F) {
					count++;
					assertEquals(col2Content, -123456789.123456D, 0.0D);
				} else if (col1Content == -10F) {
					count++;
					assertEquals(col2Content, -10D, 0.0D);
				} else if (col1Content == 10F) {
					count++;
					assertEquals(col2Content, 10D, 0.0D);
				} else if (col1Content == 123.456F) {
					count++;
					assertEquals(col2Content, 123456789.123456D, 0.0D);
				}
			}
			assertEquals(4, count);
		}
	}
	
	TableMeta getEnumTypeTableMeta() {
		ColumnMeta pk = newFixLengthColumnMeta(UNSIGNED_SMALLINT, 1, "film_id", false);
		TableMeta tableMeta = new TableMeta()
				.addColumn(pk)
				.setClusterKey(1, pk)
				.addColumn(newTrxIdColumnMeta(2))
				.addColumn(newRollPtrColumnMeta(3))
				.addColumn(newEnumColumnMeta(4, "rating", true, "G", "PG", "PG-13", "R", "NC-17"));
		return tableMeta;
	}
	
	/**
	 * root@localhost [testcase]> select * from enum_type;
	 * +---------+--------+
	 * | film_id | rating |
	 * +---------+--------+
	 * |       1 | PG     |
	 * |       2 | G      |
	 * |       3 | NC-17  |
	 * |       4 | G      |
	 * |       5 | G      |
	 * |       6 | PG     |
	 * |       7 | PG-13  |
	 * |       8 | R      |
	 * |       9 | PG-13  |
	 * |      10 | NC-17  |
	 * |      11 | NULL   |
	 * |      12 | PG     |
	 * +---------+--------+
	 * 12 rows in set (0.01 sec)
	 * 
	 * @throws Exception
	 */
	
	@Test
	void testEnumType() throws Exception {
		TableMeta tableMeta = getEnumTypeTableMeta();
		String fileName = "src/test/cases/ibd/8.0.18/enum_type.ibd";
		try (IbdFileParser ibdFileParser = new IbdFileParser(fileName)) {
			Page page = ibdFileParser.getPage(4);
			ClusteredKeyLeafPage indexPage = new ClusteredKeyLeafPage(page.getPageRaw(), page.getPageSize());
			List<ClusteredKeyLeafRecord> userRecords = indexPage.getUserRecords(tableMeta);
			//System.out.println(toReadableString(userRecords, true));
			for (ClusteredKeyLeafRecord userRecord : userRecords) {
				List<RecordField> fields = userRecord.getRecordFields();
				int fieldSize = fields.size();
				assertEquals(fieldSize, tableMeta.getColumnCount());
				RecordField pkField = fields.get(0);
				assertEquals(userRecords.size(), 12);
				int valuePk = ((BigInteger) pkField.getContent()).intValue();
				RecordField rating = fields.get(3);
				switch (valuePk) {
				case 1:
					assertEquals(rating.getContent(), "PG");
					break;
				case 2:
					assertEquals(rating.getContent(), "G");
					break;
				case 3:
					assertEquals(rating.getContent(), "NC-17");
					break;
				case 4: case 5: case 6:
					break;
				case 7:
					assertEquals(rating.getContent(), "PG-13");
					break;
				case 8:
					assertEquals(rating.getContent(), "R");
					break;
				case 9: case 10: 
					break;
				case 11:
					assertNull(rating.getContent());
					break;
				case 12:
					assertEquals(rating.getContent(), "PG");
					break;
				default: 
					throw new Exception();
				}
			}
		}
	}

	TableMeta getSetTypeTableMeta() {
		ColumnMeta pk = newFixLengthColumnMeta(UNSIGNED_SMALLINT, 1, "film_id", false);
		TableMeta tableMeta = new TableMeta()
				.addColumn(pk)
				.setClusterKey(1, pk)
				.addColumn(newTrxIdColumnMeta(2))
				.addColumn(newRollPtrColumnMeta(3))
				.addColumn(newSetColumnMeta(4, "special_features", true, "Trailers","Commentaries","Deleted Scenes","Behind the Scenes"));
		return tableMeta;
	}
	
	@Test
	void testSetType() throws Exception {
		TableMeta tableMeta = getSetTypeTableMeta();
		String fileName = "src/test/cases/ibd/8.0.18/set_type.ibd";
		try (IbdFileParser ibdFileParser = new IbdFileParser(fileName)) {
			Page page = ibdFileParser.getPage(4);
			ClusteredKeyLeafPage indexPage = new ClusteredKeyLeafPage(page.getPageRaw(), page.getPageSize());
			List<ClusteredKeyLeafRecord> userRecords = indexPage.getUserRecords(tableMeta);
			// System.out.println(dumpClusteredKeyLeafRecords(userRecords, true));
			for (ClusteredKeyLeafRecord record : userRecords) {
				List<RecordField> fields = record.getRecordFields();
				RecordField filmId = fields.get(0);
				RecordField specialFeatures = fields.get(3);
				long filmIdContent = ((BigInteger) filmId.getContent()).longValueExact();
				if (filmIdContent == 1) {
					assertEquals(specialFeatures.getContent().toString(), "[Deleted Scenes, Behind the Scenes]");
				} else if (filmIdContent == 2) {
					assertEquals(specialFeatures.getContent().toString(), "[Trailers, Deleted Scenes]");
				} else if (filmIdContent == 4) {
					assertEquals(specialFeatures.getContent().toString(), "[Commentaries, Behind the Scenes]");
				} else if (filmIdContent == 5) {
					assertEquals(specialFeatures.getContent().toString(), "[Deleted Scenes]");
				}
			}
		}
	}

	TableMeta getCharTypeTableMeta() {
		ColumnMeta col1 = ColumnMeta.newFixLengthColumnMeta(INT, 1, "col1", false);
		ColumnMeta trxid = ColumnMeta.newTrxIdColumnMeta(2);
		ColumnMeta rollPrt = ColumnMeta.newRollPtrColumnMeta(3);
		ColumnMeta col2 = ColumnMeta.newColumnMeta(CHAR, 4, "col2", 8, true, false);
		ColumnMeta col3 = ColumnMeta.newColumnMeta(CHAR, 5, "col3", 255, true, false);
		ColumnMeta col4 = ColumnMeta.newColumnMeta(CHAR, 6, "col4", 8, true, true);
		ColumnMeta col5 = ColumnMeta.newColumnMeta(CHAR, 7, "col5", 255 * 4, true, true);
		TableMeta tableMeta = new TableMeta()
				.addColumn(col1)
				.addColumn(trxid)
				.addColumn(rollPrt)
				.addColumn(col2)
				.addColumn(col3)
				.addColumn(col4)
				.addColumn(col5)
				.setClusterKey(1, col1);
		return tableMeta;
	}

	@Test
	@Disabled
	//Windows平台测试会乱码, Eclipse和Linux没问题, 应该和字符编码有关。
	void testCharType() throws IOException, Exception {
		String fileName = "src/test/cases/ibd/8.0.18/char_type.ibd";
		TableMeta tableMeta = getCharTypeTableMeta();
		try (IbdFileParser ibdFileParser = new IbdFileParser(fileName)) {
			Page page = ibdFileParser.getPage(4);
			ClusteredKeyLeafPage page4 = new ClusteredKeyLeafPage(page.getPageRaw(), page.getPageSize());
			List<ClusteredKeyLeafRecord> records = page4.getUserRecords(tableMeta);
			for (ClusteredKeyLeafRecord record : records) {
				List<RecordField> fields = record.getRecordFields();
				long col1 = ((BigInteger) fields.get(0).getContent()).longValueExact();
				// 1 trxid 
				// 2 roll_prt
				Object col2 = fields.get(3).getContent();
				Object col3 = fields.get(4).getContent();
				Object col4 = fields.get(5).getContent();
				Object col5 = fields.get(6).getContent();
				if(col1 == 1L) {
					assertEquals(col2, "col2-aa ");
					assertEquals(col3, "col3-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
					assertEquals(col4, "col4-测测");
					assertEquals(col5, "col5-测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测测");
				} else if(col1 == 2L) {
					assertEquals(col2, "col2-bb ");
					assertEquals(col3, "col3-bb                                                                                                                                                                                                                                                        ");
					assertEquals(col4, "col4-试试");
					assertEquals(col5.toString(), "col5-试试                                                                                                                                                                                                                                                    ");
				} else if(col1 == 3L) {
					assertEquals(col2, "col2-cc ");
					assertNull(col3);
					assertEquals(col4, "col4-案案");
					assertNull(col5);
				}
			}
		}
	}
}
