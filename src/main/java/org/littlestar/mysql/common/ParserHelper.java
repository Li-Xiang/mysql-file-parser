package org.littlestar.mysql.common;

import java.math.BigInteger;
import java.util.BitSet;
import java.util.Objects;

public class ParserHelper {
	private ParserHelper() {}

	/**
	 * Get the BitSet of byte.
	 * 
	 * @param b the byte.
	 * 
	 * @return the BitSet of byte.
	 * 
	 * @see {@link #toBitSet(byte[])}
	 */
	public static BitSet toBitSet(byte b) {
		byte[] bytes = new byte[] { b };
		return toBitSet(bytes);
	}

	/**
	 * Get the BitSet of byte array.
	 * 
	 * @param bytes the byte array
	 * @return the BitSet of byte array.
	 */
	public static BitSet toBitSet(byte[] bytes) {
		BitSet bs = new BitSet();
		for (int i = 0; i < bytes.length * 8; i++) {
			if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
				bs.set(i);
			}
		}
		return bs;
	}

	/**
	 * Get the BitSet of byte array between fromIndex and toIndex
	 * 
	 * @param bytes     the byte array
	 * @param fromIndex index of the first bit to include
	 * @param toIndex   index after the last bit to include
	 * 
	 * @return the BitSet of byte array between fromIndex and toIndex.
	 * @see {@link #toBitSet(byte[])}
	 */
	public static BitSet toBitSet(byte[] bytes, int fromIndex, int toIndex) {
		BitSet bs = toBitSet(bytes);
		return bs.get(fromIndex, toIndex);
	}
	
	public static BitSet toBitSet(byte b, int fromIndex, int toIndex) {
		return toBitSet(new byte[] { b }, fromIndex, toIndex);
	}

	public static boolean isSet(byte[] bytes, int bit) {
		BitSet bs = toBitSet(bytes);
		return bs.get(bit);
	}
	
	public static long getLong(BitSet bits) {
		long value = 0L;
		for (int i = 0; i < bits.length(); ++i) {
			value += bits.get(i) ? (1L << i) : 0L;
		}
		return value;
	}
	
	/**
	 * Get the binary string of BitSet.
	 * 
	 * <pre>
	 * index:       ... 7 6 5 4 3 2 1 0
	 * 0b01011111 = ... 0 1 0 1 1 1 1 1
	 * </pre>
	 * 
	 * @param bs        the BitSet to be convert to binary string.
	 * @param fromIndex index of the first bit to include
	 * @param toIndex   index after the last bit to include
	 * @return binary String of BitSet.
	 * 
	 */
	public static String toBinaryString(BitSet bs, int fromIndex, int toIndex) {
		StringBuilder buff = new StringBuilder();
		for (int i = toIndex; i >= fromIndex; i--) {
			buff.append(bs.get(i) ? "1" : "0");
		}
		return buff.toString();
	}

	public static String toBinaryString(byte b) {
		if (Objects.isNull(b)) {
			throw new IllegalArgumentException("Input byte is null.");
		}
		return String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
	}

	public static String toBinaryString(byte[] bytes) {
		StringBuilder buff = new StringBuilder();
		for (byte b : bytes) {
			buff.append(toBinaryString(b));
		}
		return buff.toString();
	}

	public static String toHexString(byte bytes) {
		StringBuilder buff = new StringBuilder();
		int v = bytes & 0xFF;
		String hv = Integer.toHexString(v);
		if (hv.length() < 2) {
			buff.append(0);
		}
		buff.append(hv);
		return buff.toString();
	}

	/**
	 * bytes[0], bytes[1], bytes[2], ....
	 * 
	 * @param bytes
	 * 
	 * @return
	 */
	public static String toHexString(byte[] bytes) {
		if (Objects.isNull(bytes) || bytes.length < 0) {
			throw new IllegalArgumentException("Input bytes is null or length <= 0.");
		}
		StringBuilder buff = new StringBuilder();
		for (int i = 0; i < bytes.length; i++) {
			int v = bytes[i] & 0xFF;
			String hv = Integer.toHexString(v);
			if (hv.length() < 2) {
				buff.append(0);
			}
			buff.append(hv);
		}
		return buff.toString();
	}

	public static char toAscii(byte b, char placeholder) {
		if ((b < 32) || (b > 126)) {
			return placeholder;
		} else {
			return (char) b;
		}
	}

	public static char toAscii(byte b) {
		return toAscii(b, '.');
	}

	public static String hexDump(byte[] bytes, int rowlen, boolean withAscii) {
		StringBuilder buff = new StringBuilder();
		String nl = System.lineSeparator();
		int pos = 0;
		int rows = bytes.length / rowlen;
		StringBuilder ascii = new StringBuilder();
		for (int i = 0; i < rows; i++) {
			buff.append(lpad(Integer.toHexString((i) * rowlen), 8, '0')).append("h: ");
			for (int j = 0; j < rowlen; j++) {
				final byte byteValue = bytes[pos++];
				String hexValue = toHexString(byteValue);
				buff.append(hexValue).append(" ");
				if (withAscii) {
					ascii.append(toAscii(byteValue));
				}
			}
			if (withAscii) {
				buff.append("    |").append(ascii.toString()).append("|");
				ascii.setLength(0);
			}
			buff.append(nl);
		}
		// less bytes.
		if (pos < bytes.length) {
			buff.append(lpad(Integer.toHexString((rows) * rowlen), 8, '0')).append("h: ");
			for (int k = pos; k < bytes.length; k++) {
				byte byteValue = bytes[pos++];
				String hexValue = toHexString(byteValue);
				buff.append(hexValue).append(" ");
			}
			buff.append(nl);
		}
		return buff.toString();
	}

	public static String hexDump(byte[] bytes, boolean withAscii) {
		return hexDump(bytes, 16, withAscii);
	}
	
	public static String hexDump(byte[] bytes) {
		return hexDump(bytes, 16, true);
	}

	private static String lpad(String s, int len, char pad) {
		StringBuilder buff = new StringBuilder();
		while ((s.length() + buff.length()) < len) {
			buff.append(pad);
		}
		buff.append(s);
		return buff.toString();
	}

	public static byte[] getReverse(byte[] bytes) {
		byte[] newBytes = bytes.clone();
		for (int i = 0, length = newBytes.length >> 1; i < length; i++) {
			int j = newBytes.length - 1 - i;
			byte t = newBytes[i];
			newBytes[i] = newBytes[j];
			newBytes[j] = t;
		}
		return newBytes;
	}

	////// UInt8/Int8 //////
	public static int getInt8(byte b) {
		int value = (int) b;
		return value;
	}

	public static int getUInt8(byte b) {
		int value = b & 0xFF;
		return value;
	}

	public static byte int8ToByte(Short value) {
		if (-128 > value || value > 127) {
			throw new IllegalArgumentException("Out of Range Int8[-128 127].");
		}
		return value.byteValue();
	}

	public static byte uint8ToByte(short value) {
		if (0 > value || value > 255) {
			throw new IllegalArgumentException("Out of Range UInt8[0 255].");
		}
		return (byte) (value & 0xFF);
	}

	////// UInt16/Int16 //////
	public static int getInt16(byte[] bytes) {
		if (Objects.isNull(bytes) || bytes.length != 2) {
			throw new IllegalArgumentException("Input bytes is null or length != 2.");
		}
		return (bytes[0] << 8) | (bytes[1] & 0xFF);
	}

	public static int getUInt16(byte[] bytes) {
		if (Objects.isNull(bytes) || bytes.length != 2) {
			throw new IllegalArgumentException("Input bytes is null or length != 2.");
		}
		int value = 0;
		for (byte b : bytes) {
			value = (value << 8) + (b & 0xFF);
		}
		return value;
	}

	public static byte[] int16ToBytes(short value) {
		return new byte[] { (byte) (value >> 8 & 0xFF), (byte) (value & 0xFF) };
	}
	
	public static int getInt24(byte[] bytes) {
		if (Objects.isNull(bytes) || bytes.length != 3) {
			throw new IllegalArgumentException("Input bytes is null or length != 3.");
		}
		return bytes[0] << 16 | (bytes[1] & 0xFF) << 8 | (bytes[2] & 0xFF);
	}

	public static int getUInt24(byte[] bytes) {
		if (Objects.isNull(bytes) || bytes.length != 3) {
			throw new IllegalArgumentException("Input bytes is null or length != 3.");
		}
		int value = 0;
		for (byte b : bytes) {
			value = (value << 8) + (b & 0xFF);
		}
		return value;
		
	}
	
	public static int getInt32(byte[] bytes) {
		return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
	}

	public static long getUInt32(byte[] bytes) {
		return getInt32(bytes) & 0xFFFFFFFFL;
	}
	
	public static long getInt64(byte[] bytes) {
		if (Objects.isNull(bytes) || bytes.length != 8) {
			throw new IllegalArgumentException("input bytes is null or length != 8.");
		}
		long value = 0L;
		for (byte b : bytes) {
			value = (value << 8) + (b & 255);
		}
		return value;
	}

	public static BigInteger getUInt64(byte[] bytes) {
		if (Objects.isNull(bytes) || bytes.length != 8) {
			throw new IllegalArgumentException("input bytes is null or length != 8.");
		}
		return new BigInteger(1, bytes);
	}
}
