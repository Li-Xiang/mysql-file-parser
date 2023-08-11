package org.littlestar.mysql.ibd.page;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

import static org.littlestar.mysql.common.ParserHelper.*;

public class RecordExtra {
	private byte[] recordExtraRaw;
	private int nextPos;
	public RecordExtra() {}

	public RecordExtra(byte[] recordExtraRaw) {
		this.recordExtraRaw = recordExtraRaw;
	}

	public byte[] getRecordExtraRaw() {
		return recordExtraRaw;
	}

	/**
	 * REC_INFO_INSTANT_FLAG: The instant ADD COLUMN flag. When it is set to 1, it
	 * means this record was inserted/updated after an instant ADD COLUMN.
	 * 
	 * @return
	 */
	public boolean getInstantFlag() {
		if (Objects.isNull(recordExtraRaw)) {
			throw new RuntimeException("recordExtraRaw is null.");
		}
		BitSet bs = toBitSet(getRecordExtraRaw()[0]);
		int deleteFlagIndex = 7;
		return bs.get(deleteFlagIndex);
	}

	/**
	 * REC_INFO_DELETED_FLAG: The deleted flag in info bits; when bit is set to 1,
	 * it means the record has been delete marked.
	 * 
	 * @return the deleted_flag in recored extra.
	 */
	public boolean getDeletedFlag() {
		if (Objects.isNull(recordExtraRaw)) {
			throw new RuntimeException("recordExtraRaw is null.");
		}
		BitSet bs = toBitSet(getRecordExtraRaw()[0]);
		int deleteFlagIndex = 5;
		return bs.get(deleteFlagIndex);
	}

	/**
	 * REC_INFO_MIN_REC_FLAG:
	 * 
	 * @return
	 */
	public boolean getMinRecFlag() {
		if (Objects.isNull(recordExtraRaw)) {
			throw new RuntimeException("recordExtraRaw is null.");
		}
		BitSet bs = toBitSet(getRecordExtraRaw()[0]);
		int miniRecFlagIndex = 4;
		return bs.get(miniRecFlagIndex);
	}

	/**
	 * REC_NEW_N_OWNED: Number of records owned (4 bits)
	 */

	public int getRecordsOwned() {
		if (Objects.isNull(recordExtraRaw)) {
			throw new RuntimeException("recordExtraRaw is null.");
		}
		int fromIndex = 0;
		int toIndex = 3;
		BitSet bs = toBitSet(getRecordExtraRaw()[0], fromIndex, toIndex);
		// return ParserHelper.toBinaryString(bs, 0, 3);
		return (int) getLong(bs);
	}

	public int getRecordHeapNo() {
		if (Objects.isNull(recordExtraRaw)) {
			throw new RuntimeException("recordExtraRaw is null.");
		}
		byte[] bytes = new byte[] { recordExtraRaw[1], recordExtraRaw[2] };
		int fromIndex = 3;
		int toIndex = 15;
		BitSet bs = toBitSet(bytes, fromIndex, toIndex);
		return (int) getLong(bs);
	}

	public int getRecordStatus() {
		if (Objects.isNull(recordExtraRaw)) {
			throw new RuntimeException("recordExtraRaw is null.");
		}
		byte[] bytes = new byte[] { recordExtraRaw[2] };
		int fromIndex = 0;
		int toIndex = 2;
		BitSet bs = toBitSet(bytes, fromIndex, toIndex);
		return (int) getLong(bs);
	}

	/**
	 * the next_record_offset in record extra.
	 * 
	 * @return the next_record_offset in record extra.
	 */
	public int getNextRecordOffset() {
		if (Objects.isNull(recordExtraRaw)) {
			throw new RuntimeException("recordExtraRaw is null.");
		}
		int from = recordExtraRaw.length - 2;
		int to = recordExtraRaw.length;
		byte[] nextRecordOffset = Arrays.copyOfRange(recordExtraRaw, from, to);
		return (nextRecordOffset[0] << 8 | nextRecordOffset[1] & 0xFF);
	}

	public int getNextRecordPos() {
		return nextPos;
	}
	
	public void setNextRecordPos(int pos) {
		nextPos = pos;
	}
	
	public void setRecordExtraRaw(byte[] recordExtraRaw) {
		this.recordExtraRaw = recordExtraRaw;
	}
	
	public static String getRecordType(int status) {
		switch (status) {
		case 0: return "REC_STATUS_ORDINARY";
		case 1: return "REC_STATUS_NODE_PTR";
		case 2: return "REC_STATUS_INFIMUM";
		case 3: return "REC_STATUS_SUPREMUM";
		default:return "UNKNOWN(" + status + ")";
		}
	}
}
