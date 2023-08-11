package org.littlestar.mysql.ibd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.newColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.newFixLengthColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.newRollPtrColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnMeta.newTrxIdColumnMeta;
import static org.littlestar.mysql.ibd.parser.ColumnType.INT;
import static org.littlestar.mysql.ibd.parser.ColumnType.VARCHAR;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage;
import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage.ClusteredKeyLeafRecord;
import org.littlestar.mysql.ibd.page.ClusteredKeyNonLeafPage.ClusteredKeyNonLeafRecord;
import org.littlestar.mysql.ibd.page.ClusteredKeyNonLeafPage;
import org.littlestar.mysql.ibd.page.IndexPage;
import org.littlestar.mysql.ibd.page.IndexPage.IndexHeader;
import org.littlestar.mysql.ibd.page.IndexPage.RecordField;
import org.littlestar.mysql.ibd.page.Page;
import org.littlestar.mysql.ibd.page.SdiPage;
import org.littlestar.mysql.ibd.page.SdiPage.SdiRecord;
import org.littlestar.mysql.ibd.parser.ColumnMeta;
import org.littlestar.mysql.ibd.parser.IbdFileParser;
import org.littlestar.mysql.ibd.parser.TableMeta;

/**
 * CREATE TABLE `testcase`.`composite_key` (
 *   `pk1` varchar(200) NOT NULL,
 *   `pk2` varchar(8) NOT NULL,
 *   `sk1` varchar(200) DEFAULT NULL,
 *   `sk2` int DEFAULT NULL,
 *   PRIMARY KEY (`pk1`,`pk2`),  
 *   KEY `sk1` (`sk1`,`sk2`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 * 
 * INSERT testcase.composite_key SELECT email, substring(first_name, 1, 8), last_name, customer_id FROM sakila.customer;
 * UPDATE testcase.composite_key SET sk1 = null WHERE sk2 = 367;
 * UPDATE testcase.composite_key SET sk2 = null WHERE sk2 = 217;
 */

class CompositeKeyTest {
	final String ibdFile = "src/test/cases/ibd/8.0.18/composite_key.ibd";
	
	final ColumnMeta pk1 = newColumnMeta(VARCHAR, 1, "pk1", 800, false, true);
	final ColumnMeta pk2 = newColumnMeta(VARCHAR, 2, "pk2", 32, false, true);
	final ColumnMeta sk1 = newColumnMeta(VARCHAR, 5, "sk1", 800, true, true);
	final ColumnMeta sk2 = newFixLengthColumnMeta(INT, 6, "sk2", true);
	
	final int sdiPageNo = 3;
	final int clusterKeyNonLeafPageNo   = 4;
	final int secondaryKeyNonLeafPageNo = 5;
	final int clusterKeyLeafPageNo   = 6;
	final int secondaryKeyLeafPageNo = 8;
	
	final long clusterKeyId   = 2005;
	final long secondaryKeyId = 2006;
	final int nonLeafLevel   = 1;
	final int leafLevel      = 0;
	
	TableMeta getTableMeta() {
		TableMeta tableMeta = new TableMeta()
				.addColumn(pk1)
				.addColumn(pk2)
				.addColumn(newTrxIdColumnMeta(3))
				.addColumn(newRollPtrColumnMeta(4))
				.addColumn(sk1)
				.addColumn(sk2)
				.setClusterKey(1, pk1)
				.setClusterKey(2, pk2)
				.setSecondaryKey(secondaryKeyId, 1, sk1)
				.setSecondaryKey(secondaryKeyId, 1, sk2);
		return tableMeta;
	}
	
	@Test
	void testSdiPage() throws IOException, Exception {
		try (IbdFileParser parser = new IbdFileParser(ibdFile)) {
			SdiPage sidPage = (SdiPage) parser.getPage(sdiPageNo);
			List<SdiRecord> records = sidPage.getUserRecords();
			for(SdiRecord record: records) {
				byte[] unZipData = record.getUnZipDataRaw();
				byte[] zipData = record.getZipDataRaw();
				//System.out.println("type: " + record.getType());
				//System.out.println("id: " + record.getId());
				//System.out.println("unzip-len:" + record.getUncompressedLen() +"; real: "+unzipData.length);
				//System.out.println(ParserHelper.toHexString(unzipData));
				//System.out.println("=>"+new String(unzipData).trim()+"<=");
				//System.out.println("zip-len:" + record.getCompressedLen() +"; real: "+zipData.length);
				//System.out.println(ParserHelper.toHexString(zipData));
				long unzipLen = record.getUncompressedLen();
				long zipLen = record.getCompressedLen();
				Assertions.assertEquals(unzipLen, unZipData.length);
				Assertions.assertEquals(zipLen, zipData.length);
			}
		}
	}
	
	@Test 
	void testIbdFileParse() throws IOException, Exception {
		try (IbdFileParser parser = new IbdFileParser(ibdFile)) {
			//System.out.println(dumpIndexPages(ibdFileParser));
			Page clusterKeyNonLeafPage = parser.getPage(clusterKeyNonLeafPageNo);
			Page secondaryKeyNonLeafPage = parser.getPage(secondaryKeyNonLeafPageNo);
			Page clusterKeyLeafPage = parser.getPage(clusterKeyLeafPageNo);
			Page secondaryKeyLeafPage = parser.getPage(secondaryKeyLeafPageNo);
			assertInstanceOf(IndexPage.class, clusterKeyNonLeafPage);
			assertInstanceOf(IndexPage.class, secondaryKeyNonLeafPage);
			assertInstanceOf(IndexPage.class, clusterKeyLeafPage);
			assertInstanceOf(IndexPage.class, secondaryKeyLeafPage);
			
			IndexPage clusterKeyNonLeafIndexPage = (IndexPage) clusterKeyNonLeafPage;
			IndexPage secondaryKeyNonLeafIndexPage = (IndexPage) secondaryKeyNonLeafPage;
			IndexPage clusterKeyLeafIndexPage = (IndexPage) clusterKeyLeafPage;
			IndexPage secondaryKeyLeafIndexPage = (IndexPage) secondaryKeyLeafPage;
			
			IndexHeader clusterKeyNonLeafIndexHeader = clusterKeyNonLeafIndexPage.getIndexHeader();
			IndexHeader secondaryKeyNonLeafIndexHeader = secondaryKeyNonLeafIndexPage.getIndexHeader();
			IndexHeader clusterKeyLeafIndexHeader = clusterKeyLeafIndexPage.getIndexHeader();
			IndexHeader secondaryKeyLeafIndexHeader = secondaryKeyLeafIndexPage.getIndexHeader();
			
			assertEquals(clusterKeyNonLeafIndexHeader.getIndexId().longValueExact(), clusterKeyId);
			assertEquals(secondaryKeyNonLeafIndexHeader.getIndexId().longValueExact(), secondaryKeyId);
			assertEquals(clusterKeyLeafIndexHeader.getIndexId().longValueExact(), clusterKeyId);
			assertEquals(secondaryKeyLeafIndexHeader.getIndexId().longValueExact(), secondaryKeyId);
			
			assertEquals(clusterKeyNonLeafIndexHeader.getPageLevel(), nonLeafLevel);
			assertEquals(secondaryKeyNonLeafIndexHeader.getPageLevel(), nonLeafLevel);
			assertEquals(clusterKeyLeafIndexHeader.getPageLevel(), leafLevel);
			assertEquals(secondaryKeyLeafIndexHeader.getPageLevel(), leafLevel);
		}
	}
	
	/**
	 * <pre>
	 * select * from testcase.composite_key where pk1 in(
	 * 	'ALEXANDER.FENNELL@sakilacustomer.org', 
	 * 	'ADAM.GOOCH@sakilacustomer.org',
	 * 	'AGNES.BISHOP@sakilacustomer.org') ;
	 * 
	 * pk1                                 |pk2     |sk1    |sk2|
	 * ------------------------------------+--------+-------+---+
	 * ADAM.GOOCH@sakilacustomer.org       |ADAM    |       |367|
	 * AGNES.BISHOP@sakilacustomer.org     |AGNES   |BISHOP |   |
	 * ALEXANDER.FENNELL@sakilacustomer.org|ALEXANDE|FENNELL|439|
	 * 
	 * </pre>
	 * 
	 * @throws Exception
	 */
	@Test
	void testClusteredKeyLeafPage() throws Exception {
		try (IbdFileParser parser = new IbdFileParser(ibdFile)) {
			Page page = parser.getPage(clusterKeyLeafPageNo);
			ClusteredKeyLeafPage indexPage = new ClusteredKeyLeafPage(page.getPageRaw(), page.getPageSize());
			//System.out.println(dumpIndexPage(indexPage, getTableMeta()));
			List<ClusteredKeyLeafRecord> userRecords = indexPage.getUserRecords(getTableMeta());
			int matchCount = 0;
			for (ClusteredKeyLeafRecord userRecord : userRecords) {
				List<RecordField> fields = userRecord.getRecordFields();
				RecordField pk1 = fields.get(0);
				RecordField pk2 = fields.get(1);
				// 2 DB_TRX_ID
				// 3 DB_ROLL_PTR
				RecordField sk1 = fields.get(4);
				RecordField sk2 = fields.get(5);
				assertEquals(pk1.getName(), "pk1");
				assertEquals(pk2.getName(), "pk2");
				assertEquals(sk1.getName(), "sk1");
				assertEquals(sk2.getName(), "sk2");
				if (Objects.equals("ADAM.GOOCH@sakilacustomer.org", pk1.getContent())) {
					matchCount++;
					assertEquals(pk2.getContent(), "ADAM");
					assertNull(sk1.getContent());
					assertEquals(((BigInteger)sk2.getContent()).intValueExact(), 367);
				} else if (Objects.equals("AGNES.BISHOP@sakilacustomer.org", pk1.getContent())) {
					matchCount++;
					assertEquals(pk2.getContent(), "AGNES");
					assertEquals(sk1.getContent(), "BISHOP");
					assertNull(sk2.getContent());
				} else if (Objects.equals("ALEXANDER.FENNELL@sakilacustomer.org", pk1.getContent())) {
					matchCount++;
					assertEquals(pk2.getContent(), "ALEXANDE");
					assertEquals(sk1.getContent(), "FENNELL");
					assertEquals(((BigInteger)sk2.getContent()).intValueExact(), 439);
				}
			}
			assertEquals(3, matchCount);
		}
	}
	
	@Test
	void testClusteredKeyNonLeafPage() throws Exception {
		try (IbdFileParser parser = new IbdFileParser(ibdFile)) {
			Page page = parser.getPage(clusterKeyNonLeafPageNo);
			ClusteredKeyNonLeafPage indexPage = new ClusteredKeyNonLeafPage(page.getPageRaw(), page.getPageSize());
			List<ClusteredKeyNonLeafRecord> userRecords = indexPage.getUserRecords(getTableMeta());
			for(ClusteredKeyNonLeafRecord record: userRecords) {
				long pageNumber = record.getChildPageNumber();
				List<RecordField> minKey = record.getMinClusterKeyOnChild();
				RecordField f0 = minKey.get(0);
				RecordField f1 = minKey.get(1);
				assertEquals(f0.getName(), "pk1");
				assertEquals(f1.getName(), "pk2");
				if (pageNumber == 6) {
					assertEquals(f0.getContent(), "AGNES.BISHOP@sakilacustomer.org");
					assertEquals(f1.getContent(), "AGNES");
				} else if (pageNumber == 11) {
					assertEquals(f0.getContent(), "DOLORES.WAGNER@sakilacustomer.org");
					assertEquals(f1.getContent(), "DOLORES");
				} else if (pageNumber == 7) {
					assertEquals(f0.getContent(), "JUANITA.MASON@sakilacustomer.org");
					assertEquals(f1.getContent(), "JUANITA");
				} else if (pageNumber == 10) {
					assertEquals(f0.getContent(), "PEARL.GARZA@sakilacustomer.org");
					assertEquals(f1.getContent(), "PEARL");
				}
			}
		}
	}
}
