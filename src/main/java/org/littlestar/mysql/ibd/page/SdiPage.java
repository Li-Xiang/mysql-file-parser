package org.littlestar.mysql.ibd.page;

import static org.littlestar.mysql.ibd.parser.ColumnMeta.*;
import static org.littlestar.mysql.common.ParserHelper.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;


import org.littlestar.mysql.ibd.page.ClusteredKeyLeafPage.ClusteredKeyLeafRecord;
import org.littlestar.mysql.ibd.parser.ColumnMeta;
import org.littlestar.mysql.ibd.parser.ColumnType;
import org.littlestar.mysql.ibd.parser.TableMeta;

public class SdiPage extends IndexPage {

	public SdiPage(byte[] pageRaw, int pageSize) {
		super(pageRaw, pageSize);
	}

	public SdiPage(byte[] pageRaw) {
		super(pageRaw);
	}
	
	public List<SdiRecord> getUserRecords() {
		ClusteredKeyLeafPage cklPage = new ClusteredKeyLeafPage(getPageRaw(), getPageSize());
		List<ClusteredKeyLeafRecord> records = cklPage.getUserRecords(getSdiTableMeta());
		List<SdiRecord> sdiRecords = new ArrayList<SdiRecord>();
		for (ClusteredKeyLeafRecord record : records) {
			SdiRecord sidRecord = new SdiRecord();
			List<RecordField> fields = record.getRecordFields();
			for (RecordField field : fields) {
				String filedName = field.getName();
				byte[] raw = field.getConetentRaw();
				if(Objects.equals(filedName, typeFieldName)) {
					sidRecord.setTypeRaw(raw);
				} else if (Objects.equals(filedName, idFieldName)){
					sidRecord.setIdRaw(raw);
				} else if(Objects.equals(filedName, trxIdFieldName)) {
					sidRecord.setTrxIdRaw(raw);
				} else if(Objects.equals(filedName, rollPrtFieldName)) {
					sidRecord.setRollPrtRaw(raw);
				} else if(Objects.equals(filedName, uncompressedLenFieldName)) {
					sidRecord.setUncompressedLenRaw(raw);
				} else if(Objects.equals(filedName, compressedLenFieldName)) {
					sidRecord.setCompressedLenRaw(raw);
				} else if(Objects.equals(filedName, zipDataFieldName)) {
					sidRecord.setZipDataRaw(raw);
				}
			}
			sdiRecords.add(sidRecord);
		}
		return sdiRecords;
	}
	
	private final String typeFieldName = "type";
	private final String idFieldName = "id";
	private final String trxIdFieldName = ColumnType.DB_TRX_ID;
	private final String rollPrtFieldName = ColumnType.DB_ROLL_PTR;
	private final String uncompressedLenFieldName = "uncompressed_len";
	private final String compressedLenFieldName = "compressed_len";
	private final String zipDataFieldName = "zip_data";
	
	private TableMeta getSdiTableMeta() {
		ColumnMeta type = newColumnMeta("RAW", 1, typeFieldName, 4, false, false);
		ColumnMeta id = newColumnMeta("RAW", 2, idFieldName, 8, false, false); 
		ColumnMeta trxId = ColumnMeta.newTrxIdColumnMeta(3);
		ColumnMeta rollPrt = ColumnMeta.newRollPtrColumnMeta(4);
		ColumnMeta uncompressedLen = newColumnMeta("RAW", 5, uncompressedLenFieldName, 4, false, false);
		ColumnMeta compressedLen = newColumnMeta("RAW", 6, compressedLenFieldName, 4, false, false);
		ColumnMeta zipData = newColumnMeta("RAW", 7, zipDataFieldName, 512, false, true);
		TableMeta tableMeta = new TableMeta()
				.addColumn(type)
				.addColumn(id)
				.addColumn(trxId)
				.addColumn(rollPrt)
				.addColumn(uncompressedLen)
				.addColumn(compressedLen)
				.addColumn(zipData)
				.setClusterKey(1, type)
				.setClusterKey(2, id);
		return tableMeta;
	}
	
	public class SdiRecord {
		private byte[] typeRaw;
		private byte[] idRaw;
		private byte[] trxIdRaw;
		private byte[] rollPrtRaw;
		private byte[] uncompressedLenRaw;
		private byte[] compressedLenRaw;
		private byte[] zipDataRaw;

		public byte[] getTypeRaw() {
			return typeRaw;
		}

		public long getType() {
			return getUInt32(typeRaw);
		}

		public void setTypeRaw(byte[] typeRaw) {
			this.typeRaw = typeRaw;
		}

		public byte[] getIdRaw() {
			return idRaw;
		}

		public BigInteger getId() {
			return getUInt64(idRaw);
		}

		public void setIdRaw(byte[] idRaw) {
			this.idRaw = idRaw;
		}

		public byte[] getTrxIdRaw() {
			return trxIdRaw;
		}

		public void setTrxIdRaw(byte[] trxIdRaw) {
			this.trxIdRaw = trxIdRaw;
		}

		public byte[] getRollPrtRaw() {
			return rollPrtRaw;
		}

		public void setRollPrtRaw(byte[] rollPrtRaw) {
			this.rollPrtRaw = rollPrtRaw;
		}

		public byte[] getUncompressedLenRaw() {
			return uncompressedLenRaw;
		}

		public long getUncompressedLen() {
			return getUInt32(uncompressedLenRaw);
		}

		public void setUncompressedLenRaw(byte[] uncompressedLenRaw) {
			this.uncompressedLenRaw = uncompressedLenRaw;
		}

		public byte[] getCompressedLenRaw() {
			return compressedLenRaw;
		}

		public long getCompressedLen() {
			return getUInt32(compressedLenRaw);
		}

		public void setCompressedLenRaw(byte[] compressedLenRaw) {
			this.compressedLenRaw = compressedLenRaw;
		}

		public byte[] getZipDataRaw() {
			return zipDataRaw;
		}

		public void setZipDataRaw(byte[] zipDataRaw) {
			this.zipDataRaw = zipDataRaw;
		}

		public byte[] getUnZipDataRaw() throws DataFormatException {
			Inflater inflater = new Inflater();
			byte[] zipData = getZipDataRaw();
			if (Objects.isNull(zipData) || zipData.length < 1) {
				return null;
			}
			inflater.setInput(zipData);
			byte[] unzipData = new byte[] {};
			byte[] buffer = new byte[1024];
			while (!inflater.finished()) {
				int count = inflater.inflate(buffer);
				int unzipLen = unzipData.length;
				byte[] temp = new byte[unzipLen + count];
				System.arraycopy(unzipData, 0, temp, 0, unzipLen);
				System.arraycopy(buffer, 0, temp, unzipLen, count);
				unzipData = temp;
			}
			return unzipData;
		}
		
		public String getData() throws DataFormatException {
			return new String(getUnZipDataRaw());
		}
	}
	

}
