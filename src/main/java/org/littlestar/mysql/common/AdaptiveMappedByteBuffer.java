package org.littlestar.mysql.common;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Objects;

/**
 * 根据position(newPos), get(dst)自动维护文件映射。
 * 
 * @author LiXiang
 *
 */
public class AdaptiveMappedByteBuffer {
	private final RandomAccessFile file;
	private final MapMode mapMode;
	
	private final int maxRangeSize;
	private long rangeNumber = 0;
	private long mapPos = 0L;
	private long mapSize = 0L;

	private MappedByteBuffer mappedBuffer;

	public AdaptiveMappedByteBuffer(RandomAccessFile file, MapMode mapMode, int maxRangeSize) throws IOException {
		this.file = file;
		this.maxRangeSize = maxRangeSize;
		this.mapMode = mapMode;
		remap(0); // init mappedBuffer
	}
	
	public long position() {
		int offset = offset();
		long newPos = mapPos + offset;
		return newPos;
	}

	public int offset() {
		if (Objects.nonNull(mappedBuffer))
			return mappedBuffer.position();
		return -1;
	}
	
	public Buffer offset(int newOffset) {
		return mappedBuffer.position(newOffset);
	}
	
	public long getMappedLowBound() {
		return mapPos;
	}
	
	public long getMappedUpBound() {
		return mapPos + mapSize;
	}
	
	public ByteBuffer get(byte[] dst) throws IOException {
		ByteBuffer byteBuffer;
		long currPos = position();
		long newEndPos = currPos + dst.length;
		if(newEndPos > getMappedUpBound()) {
			remap(currPos, currPos + dst.length);
		}
		byteBuffer = mappedBuffer.get(dst);
		return byteBuffer;
	}
	
	/**
	 *  
	 * @param newPos
	 * @return offset (relative position).
	 * @throws IOException
	 */
	public Buffer position(long newPos) throws IOException {
		if (needRemap(newPos)) {
			remap(newPos);
		}
		int offset = (int) (newPos - mapPos);
		return offset(offset);
	}
	
	private boolean needRemap(long newPos) {
		if (newPos >= (mapPos + mapSize) || newPos < mapPos) {
			return true;
		}
		return false;
	}
	
	private MappedByteBuffer remap(long startPos, long endPos) throws IOException {
		System.out.println("remap!");
		if (startPos >= endPos) {
			throw new IOException("Map startPos must larger than endPos.");
		}
		long newMapPos = startPos;
		long newMapSize = maxRangeSize;
		if (endPos > (newMapPos + newMapSize)) {
			newMapSize = startPos - endPos;
		}
		long fileLength = file.length();
		if ((startPos + newMapSize) > fileLength) {
			newMapSize = fileLength - startPos;
		}
		mappedBuffer = file.getChannel().map(mapMode, newMapPos, newMapSize);
		mapPos = newMapPos;
		mapSize = newMapSize;
		return mappedBuffer;
	}
	
	private MappedByteBuffer remap(long newPos) throws IOException {
		long newMapPos;
		long newMapSize;
		long fileLength = file.length();
		if (fileLength < maxRangeSize) { // 文件大小不超过分区最大范围，直接将将文件分区映射到MappedByteBuffer;
			newMapSize = fileLength;
			newMapPos = 0;
		} else {
			long rangeCount = newPos / maxRangeSize;
			long remain = newPos % maxRangeSize;
			if (rangeCount > 0) {
				rangeNumber = rangeCount;
				if (remain == 0) {
					newMapPos = rangeNumber * maxRangeSize;
					newMapSize = maxRangeSize;
				} else {
					newMapPos = rangeNumber * maxRangeSize;
					if ((newMapPos + maxRangeSize) < fileLength) {
						newMapSize = maxRangeSize;
					} else {
						newMapSize = fileLength - newMapPos;
					}
				}
			} else { // 映射的最大分区范围正好可以方向整个文件。
				rangeNumber = 0;
				newMapPos = 0;
				newMapSize = maxRangeSize;
			}
		}
		mappedBuffer = file.getChannel().map(mapMode, newMapPos, newMapSize);
		mapPos = newMapPos;
		mapSize = newMapSize;
		return mappedBuffer;
	}
	
	protected String bufferState() {
		return "Range: [" + getMappedLowBound() + " - " + getMappedUpBound() + "], offset: " + offset() +", position: " + position();
	}
}
