package org.littlestar.mysql.ibd.page;

import static org.littlestar.mysql.common.ParserHelper.toHexString;

import java.util.Arrays;
import java.util.Objects;

public class FilTrailer {
	private byte[] checkSum;
	private byte[] low32BitsOfLSN;

	public FilTrailer(byte[] trailerRaw) {
		if (Objects.isNull(trailerRaw) || (trailerRaw.length != 8)) {
			throw new IllegalArgumentException("passed in "+trailerRaw.length + " bytes, page triler must be 8 bytes length.");
		}
		checkSum = Arrays.copyOfRange(trailerRaw, 0, 4);
		low32BitsOfLSN = Arrays.copyOfRange(trailerRaw, 4, 8);
	}

	/**
	 * Low 4 byte of FIL_PAGE_END_LSN_OLD_CHKSUM.
	 * 
	 * the low 4 bytes of this are used to store the page checksum.
	 * 
	 * @return Low 4 byte of FIL_PAGE_END_LSN_OLD_CHKSUM.
	 */
	public byte[] getCheckSumRaw() {
		return checkSum;
	}

	/**
	 * Last 4 byte of FIL_PAGE_END_LSN_OLD_CHKSUM.
	 * 
	 * the last 4 bytes should be identical to the last 4 bytes of FIL_PAGE_LSN
	 * 
	 * @return Low 4 byte of FIL_PAGE_END_LSN_OLD_CHKSUM.
	 */
	public byte[] getLast4ByteOfLSNRaw() {
		return low32BitsOfLSN;
	}
	
	@Override
	public String toString() {
		String nl = System.lineSeparator();
		StringBuilder buff = new StringBuilder();
		buff.append("     FIL_PAGE_END_LSN_OLD_CHKSUM : ").append(nl)
			.append("                        CHECKSUM = ").append(toHexString(getCheckSumRaw())).append(nl)
			.append("    LAST 4 BYTES OF FIL_PAGE_LSN = ").append(toHexString(getLast4ByteOfLSNRaw())).append(nl);
		return buff.toString();
	}
}
