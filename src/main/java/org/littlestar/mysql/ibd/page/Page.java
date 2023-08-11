package org.littlestar.mysql.ibd.page;

import static org.littlestar.mysql.common.ParserHelper.hexDump;

import java.util.Arrays;

/**
 * 
 * <pre>
 * 0-------->+----------------------+
 *           | FIL Header (38)      |
 * 38------->+----------------------+
 *           |                      |
 * 16376---->+----------------------+
 *           |FIL Trailer (8)       |
 * 16384---->+----------------------+

 * </pre>
 * @author LiXiang
 */
public class Page {
	public static final int DEFAULT_PAGE_SIZE = 16 * 1024;
	public static final int PAGE_HEADER_POS = 0;
	public static final int PAGE_HEADER_LENGTH = 38;
	public static final int PAGE_TRAILER_LENGTH = 8;
	public static final int PAGE_PAYLOAD_POS = PAGE_HEADER_LENGTH;
	
	private final int pageSize;
	private final int headerStart  = PAGE_HEADER_POS;
	private final int headerEnd    = PAGE_HEADER_LENGTH;
	private final int payloadStart = PAGE_HEADER_LENGTH;
	private final int payloadEnd;
	private final int trailerStart;
	private final int trailerEnd;

	protected final byte[] pageRaw;
	private final FilHeader filHeader;
	private final FilTrailer filTrailer;

	public Page(byte[] pageRaw, int pageSize) {
		this.pageRaw = pageRaw;
		this.pageSize = pageSize;
		payloadEnd = getPayloadEnd(pageSize);
		trailerStart = getTrailerStart(pageSize);
		trailerEnd = getTrailerEnd(pageSize);
		filHeader = new FilHeader(getFilHeaderRaw());
		filTrailer = new FilTrailer(getFilTrailerRaw());
	}

	public int getPageSize() {
		return pageSize;
	}

	/**
	 * default 16 KiB page size;
	 */
	public Page(byte[] pageRaw) {
		this(pageRaw, DEFAULT_PAGE_SIZE);
	}

	public FilHeader getFilHeader() {
		return filHeader;
	}
	
	public byte[] getPageRaw() {
		return pageRaw;
	}

	public byte[] getFilHeaderRaw() {
		return Arrays.copyOfRange(pageRaw, headerStart, headerEnd);
	}

	public FilTrailer getFilTrailer() {
		return filTrailer;
	}

	public byte[] getFilTrailerRaw() {
		return Arrays.copyOfRange(pageRaw, trailerStart, trailerEnd);
	}

	public byte[] getPayloadRaw() {
		return Arrays.copyOfRange(pageRaw, payloadStart, payloadEnd);
	}

	public static int getPayloadEnd(int pageSize) {
		int payloadEnd = pageSize - PAGE_TRAILER_LENGTH;
		if (payloadEnd < 1) {
			throw new IllegalArgumentException(
					"page payload length < 1: pageSize=" + pageSize + ", pageTrailerSize=" + PAGE_TRAILER_LENGTH);
		}
		return payloadEnd;
	}

	public static int getTrailerStart(int pageSize) {
		return pageSize - PAGE_TRAILER_LENGTH;
	}

	public static int getTrailerEnd(int pageSize) {
		return pageSize;
	}

	@Override
	public String toString() {
		return hexDump(pageRaw);
	}
}
