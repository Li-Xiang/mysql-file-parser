package org.littlestar.mysql.ibd;

import static org.junit.jupiter.api.Assertions.*;
import static org.littlestar.mysql.common.ParserHelper.*;

import java.math.BigInteger;
import java.util.BitSet;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


class ParserHelperTest {
	@Test
	void testToBitSet() {
		byte b = (byte) 0b01011101; 
		byte[] bytes = new byte[] { b };
		BitSet bs = toBitSet(bytes);
		assertEquals(bs.get(0), true);
		assertEquals(bs.get(1), false);
		assertEquals(bs.get(2), true);
		assertEquals(bs.get(3), true);
		assertEquals(bs.get(4), true);
		assertEquals(bs.get(5), false);
		assertEquals(bs.get(6), true);
		assertEquals(bs.get(7), false);
		
		b = (byte)0b00000101;
		bs = toBitSet(b);
		assertEquals(getLong(bs), 5);
	}
	
	@Test
	@DisplayName("isSet")
	void testIsSet() {
		// 1011110101000010
		byte[] bytes = new byte[] { 
				(byte) 0b10111101, // byte[0]
				(byte) 0b01000010 // byte[1]
		};
		assertEquals(isSet(bytes, 0), false);
		assertEquals(isSet(bytes, 1), true);
		assertEquals(isSet(bytes, 2), false);
		assertEquals(isSet(bytes, 3), false);
		assertEquals(isSet(bytes, 4), false);
		assertEquals(isSet(bytes, 5), false);
		assertEquals(isSet(bytes, 6), true);
		assertEquals(isSet(bytes, 7), false);
		assertEquals(isSet(bytes, 8), true);
		assertEquals(isSet(bytes, 9), false);
		assertEquals(isSet(bytes, 10), true);
		assertEquals(isSet(bytes, 11), true);
		assertEquals(isSet(bytes, 12), true);
		assertEquals(isSet(bytes, 13), true);
		assertEquals(isSet(bytes, 14), false);
		assertEquals(isSet(bytes, 15), true);
	}
	
	@Test
	@DisplayName("UInt8/Int8")
	void testUint8() {
		byte b0 = (byte) 0x80;
		byte b1 = (byte) 0x7f;
		byte b2 = (byte) 0x00;
		byte b3 = (byte) 0x55;
		byte b4 = (byte) 0x82;
		assertEquals(getInt8(b0), Byte.MIN_VALUE);
		assertEquals(getUInt8(b0), 128);
		assertEquals(getInt8(b1), Byte.MAX_VALUE);
		assertEquals(getUInt8(b1), Byte.MAX_VALUE);
		assertEquals(getInt8(b2), 0);
		assertEquals(getUInt8(b2), 0);
		assertEquals(getInt8(b3), 85);
		assertEquals(getUInt8(b3), 85);
		assertEquals(getInt8(b4), -126);
		assertEquals(getUInt8(b4), 130);
	}

	@Test
	@DisplayName("UInt16/Int16")
	void testUint16() {
		byte[] b0 = new byte[] { (byte) 0x80, (byte) 0x00 };
		byte[] b1 = new byte[] { (byte) 0x7f, (byte) 0xff };
		byte[] b2 = new byte[] { (byte) 0x80, (byte) 0x18 };
		byte[] b3 = new byte[] { (byte) 0xff, (byte) 0xff };
		byte[] b4 = new byte[] { (byte) 0xc0, (byte) 0x18 };
		assertEquals(Short.MIN_VALUE, getInt16(b0));
		assertEquals(32768, getUInt16(b0));
		assertEquals(Short.MAX_VALUE, getInt16(b1));
		assertEquals(Short.MAX_VALUE, getUInt16(b1));
		assertEquals(-32744, getInt16(b2));
		assertEquals(32792, getUInt16(b2));
		assertEquals(-1, getInt16(b3));
		assertEquals(65535, getUInt16(b3));
		assertEquals(-16360, getInt16(b4));
		assertEquals(49176, getUInt16(b4));
	}
	
	@Test
	@DisplayName("UInt24/Int24")
	void testUInt24() {
		byte[] b0 = new byte[] { (byte) 0b10000000, (byte) 0b00000000, (byte) 0b00000000 };
		byte[] b1 = new byte[] { (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111 };
		byte[] b2 = new byte[] { (byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000 };
		byte[] b3 = new byte[] { (byte) 0b01111111, (byte) 0b11111111, (byte) 0b11111111 };
		byte[] b4 = new byte[] { (byte) 0x9b, (byte) 0xeb, (byte) 0x80 };
		byte[] b5 = new byte[] { (byte) 0x50, (byte) 0xAB, (byte) 0xF0 };
		byte[] b6 = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x06 };
		byte[] b7 = new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xf9 };
		
		assertEquals(getUInt24(b0), 8388608);
		assertEquals(getUInt24(b1), 16777215);
		assertEquals(getUInt24(b2), 0);
		assertEquals(getUInt24(b3), 8388607);
		assertEquals(getUInt24(b4), 10218368);
		assertEquals(getUInt24(b5), 5286896);
		assertEquals(getUInt24(b6), 6);
		assertEquals(getUInt24(b7), 16777209);
		
		assertEquals(getInt24(b0), -8388608);
		assertEquals(getInt24(b1), -1);
		assertEquals(getInt24(b2), 0);
		assertEquals(getInt24(b3), 8388607);
		assertEquals(getInt24(b4), -6558848);
		assertEquals(getInt24(b5), 5286896);
		assertEquals(getInt24(b7), -7);
	}
	
	@Test
	@DisplayName("UInt32/Int32")
	void testUInt32() {
		// https://simonv.fr/TypesConvert/?integers
		byte[] b0 = new byte[] { (byte) 0b10000000, (byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000 };
		byte[] b1 = new byte[] { (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111 };
		byte[] b2 = new byte[] { (byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000 };
		byte[] b3 = new byte[] { (byte) 0b01111111, (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111 };
		byte[] b4 = new byte[] { (byte) 0x9b, (byte) 0xeb, (byte) 0x80, (byte) 0xbf };
		byte[] b5 = new byte[] { (byte) 0x50, (byte) 0xAB, (byte) 0xF0, (byte) 0xBF };
		byte[] b6 = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x06 };
		byte[] b7 = new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xf9 };
		
		assertEquals(getUInt32(b0), 2147483648L);
		assertEquals(getUInt32(b1), 4294967295L);
		assertEquals(getUInt32(b2), 0L);
		assertEquals(getUInt32(b3), 2147483647L);
		assertEquals(getUInt32(b4), 2615902399L);
		assertEquals(getUInt32(b5), 1353445567);
		assertEquals(getUInt32(b6), 6);
		assertEquals(getUInt32(b7), 4294967289L);

		assertEquals(getInt32(b0), -2147483648);
		assertEquals(getInt32(b1), -1);
		assertEquals(getInt32(b2), 0);
		assertEquals(getInt32(b3), 2147483647);
		assertEquals(getInt32(b4), -1679064897);
		assertEquals(getInt32(b5), 1353445567);
		assertEquals(getInt32(b6), 6);
		assertEquals(getInt32(b7), -7);
	}
	
	@Test
	@DisplayName("UInt64/Int64")
	void testUInt64() {
		byte[] b0 = new byte[] { 
				(byte) 0b10000000, (byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000,
				(byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000 
				};
		byte[] b1 = new byte[] { 
				(byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111,
				(byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111,
				};
		byte[] b2 = new byte[] { 
				(byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000,
				(byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000, (byte) 0b00000000 
				};
		
		byte[] b3 = new byte[] { 
				(byte) 0b01111111, (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111,
				(byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111
				};
		
		byte[] b4 = new byte[] { 
				(byte) 0x9b, (byte) 0xeb, (byte) 0x80, (byte) 0xbf,
				(byte) 0x9b, (byte) 0xeb, (byte) 0x80, (byte) 0xbf,
				};
		
		byte[] b5 = new byte[] { 
				(byte) 0x50, (byte) 0xAB, (byte) 0xF0, (byte) 0xBF,
				(byte) 0x50, (byte) 0xAB, (byte) 0xF0, (byte) 0xBF
				};
		
		byte[] b6 = new byte[] {
				(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
				(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x06 
				};
		
		byte[] b7 = new byte[] { 
				(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
				(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xf9 
				};
		
		assertEquals(getInt64(b0), -9223372036854775808L);
		assertEquals(getInt64(b1), -1L);
		assertEquals(getInt64(b2), 0L);
		assertEquals(getInt64(b3), 9223372036854775807L);
		assertEquals(getInt64(b4), -7211528817860706113L);
		assertEquals(getInt64(b5), 5813004448534622399L);
		assertEquals(getInt64(b6), 6L);
		assertEquals(getInt64(b7), -7L);
		
		assertEquals(getUInt64(b0), new BigInteger("9223372036854775808"));
		assertEquals(getUInt64(b1), new BigInteger("18446744073709551615"));
		assertEquals(getUInt64(b2), new BigInteger("0"));
		assertEquals(getUInt64(b3), new BigInteger("9223372036854775807"));
		assertEquals(getUInt64(b4), new BigInteger("11235215255848845503"));
		assertEquals(getUInt64(b5), new BigInteger("5813004448534622399"));
		assertEquals(getUInt64(b6), new BigInteger("6"));
		assertEquals(getUInt64(b7), new BigInteger("18446744073709551609"));
	}
}
