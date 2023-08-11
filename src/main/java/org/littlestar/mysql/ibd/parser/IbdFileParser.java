package org.littlestar.mysql.ibd.parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.littlestar.mysql.common.AdaptiveMappedByteBuffer;
import org.littlestar.mysql.ibd.page.Page;
import org.littlestar.mysql.ibd.page.SdiPage;
import org.littlestar.mysql.ibd.page.FilHeader;
import org.littlestar.mysql.ibd.page.FspHdrPage;
import org.littlestar.mysql.ibd.page.IndexPage;

import static org.littlestar.mysql.common.ParserHelper.getUInt16;

public class IbdFileParser implements AutoCloseable {
	public static final int PAGE_SIZE_16K = 16384; // 16 * 1024;
	public static final int FIL_TRAILER_OFFSET_16K = 16376;
	/** the PAGE_TYPE start position in page, inclusive. */
	public static final int PAGE_TYPE_START_POS_IN_PAGE = 24;
	/** the PAGE_TYPE end position in page, exclusive. */
	public static final int PAGE_TYPE_END_POS_IN_PAGE   = 26;
	
	private final int maxMapSize = 1073741824; // 1024*1024*1024; //1 GiB
	
	private final RandomAccessFile ibdRaf;
	private final AdaptiveMappedByteBuffer mappedByteBuffer;
	private final int pageSize;
	
	public IbdFileParser(String ibdFileName, int pageSize) throws IOException {
		this.pageSize = pageSize;
		ibdRaf = new RandomAccessFile(ibdFileName, "r");
		mappedByteBuffer = new AdaptiveMappedByteBuffer(ibdRaf, MapMode.READ_ONLY, maxMapSize);
	}
	
	public IbdFileParser(String ibdFileName) throws IOException {
		this(ibdFileName, PAGE_SIZE_16K);
	}
	
	/**
	 * get the PageType, PageType's Page Index map of file: Map(PageType,
	 * List[PageIndex]), thread not safe.
	 * 
	 */
	public Map<Integer, List<Long>> getPageTypeMap() throws IOException {
		final Map<Integer, List<Long>> pageTypeMap = new HashMap<Integer, List<Long>>();
		final long fileSize = getFileLength();
		long pageIndex = 0;
		long pos = 0L;
		while (true) {
			pos = getPageStartPos(pageIndex);
			if (pos >= fileSize) {
				break;
			}
			byte[] pageTypeRaw = new byte[PAGE_TYPE_END_POS_IN_PAGE - PAGE_TYPE_START_POS_IN_PAGE];
			mappedByteBuffer.position(pos + PAGE_TYPE_START_POS_IN_PAGE);
			mappedByteBuffer.get(pageTypeRaw);
			int pageType = getUInt16(pageTypeRaw);
			List<Long> pages = pageTypeMap.get(pageType);
			if (Objects.isNull(pages)) {
				pages = new ArrayList<Long>();
				pageTypeMap.put(pageType, pages);
			}
			pages.add(pageIndex);
			pageIndex++;
		}
		return pageTypeMap;
	}
	
	public long getPageCount() throws IOException {
		return getFileLength()/pageSize;
	}
	
	public long getFileLength() throws IOException {
		return ibdRaf.length();
	}
	
	/**
	 * The giving page index of page's start position (inclusive).
	 */
	public long getPageStartPos(long pageIndex) {
		return pageIndex * pageSize;
	}

	/**
	 * The giving page index of page's end position (exclusive).
	 */
	public long getPageEndPos(long pageIndex) {
		return getPageStartPos(pageIndex) + pageSize;
	}

	/**
	 * thread not safe.
	 * 
	 * @param pageIndex
	 * @return
	 * @throws IOException
	 */
	public Page getPage(long pageIndex) throws IOException {
		long pageStartPos = getPageStartPos(pageIndex);
		mappedByteBuffer.position(pageStartPos);
		byte[] pageRaw = new byte[pageSize];
		mappedByteBuffer.get(pageRaw);
		
		byte[] pageTypeRaw = Arrays.copyOfRange(pageRaw, PAGE_TYPE_START_POS_IN_PAGE, PAGE_TYPE_END_POS_IN_PAGE);
		int pageType = getUInt16(pageTypeRaw);
		switch (pageType) {
		case FilHeader.FIL_PAGE_TYPE_FSP_HDR:
			return new FspHdrPage(pageRaw, pageSize);
		case FilHeader.FIL_PAGE_INDEX:
			return new IndexPage(pageRaw, pageSize);
		case FilHeader.FIL_PAGE_SDI:
			return new SdiPage(pageRaw, pageSize);
		default:
			return new Page(pageRaw, pageSize);
		}
	}
	
	@Override
	public void close() throws Exception {
		if (Objects.nonNull(ibdRaf)) {
			ibdRaf.close();
		}
	}
}
