package org.littlestar.mysql.ibd.parser;

import static org.littlestar.mysql.common.ParserHelper.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

/**
 * Numeric Data Types.
 * <pre>
 * data_type            fixed_length
 * -------------------- -------------
 * TINYINT              1
 * TINYINT UNSIGNED     1
 * SMALLINT             2
 * SMALLINT UNSIGNED    2
 * MEDIUMINT            3
 * </pre>
 * 
 * Date and Time Data Types.
 * <pre>
 * data_type            fixed_length
 * -------------------- -------------
 * DATE                 3
 * TIME                 3
 * DATETIME             8
 * TIMESTAMP            4
 * YEAR                 1
 * </pre>
 * 
 */
public final class ColumnType {
	private ColumnType() {}

	public static final String BIT = "BIT";
	public static final String BOOL = "BOOL";
	public static final String BOOLEAN = "BOOLEAN";
	public static final String ENUM = "ENUM";
	public static final String SET = "SET";

	public static final String TINYINT = "TINYINT";
	public static final String SMALLINT = "SMALLINT";
	public static final String MEDIUMINT = "MEDIUMINT";
	public static final String INT = "INT";
	public static final String BIGINT = "BIGINT";

	public static final String UNSIGNED_TINYINT = "TINYINT UNSIGNED";
	public static final String UNSIGNED_SMALLINT = "SMALLINT UNSIGNED";
	public static final String UNSIGNED_MEDIUMINT = "MEDIUMINT UNSIGNED";
	public static final String UNSIGNED_INT = "INT UNSIGNED";
	public static final String UNSIGNED_BIGINT = "BIGINT UNSIGNED";

	public static final String UNSIGNED_FLOAT = "FLOAT UNSIGNED";
	public static final String UNSIGNED_REAL = "REAL UNSIGNED";
	public static final String UNSIGNED_DOUBLE = "DOUBLE UNSIGNED";
	public static final String UNSIGNED_DECIMAL = "DECIMAL UNSIGNED";
	public static final String UNSIGNED_NUMERIC = "NUMERIC UNSIGNED";

	public static final String FLOAT = "FLOAT";
	public static final String REAL = "REAL";
	public static final String DOUBLE = "DOUBLE";
	public static final String DECIMAL = "DECIMAL";
	public static final String NUMERIC = "NUMERIC";

	public static final String CHAR = "CHAR";
	public static final String VARCHAR = "VARCHAR";
	public static final String BINARY = "BINARY";
	public static final String VARBINARY = "VARBINARY";
	public static final String TINYBLOB = "TINYBLOB";
	public static final String BLOB = "BLOB";
	public static final String MEDIUMBLOB = "MEDIUMBLOB";
	public static final String LONGBLOB = "LONGBLOB";
	public static final String TINYTEXT = "TINYTEXT";
	public static final String TEXT = "TEXT";
	public static final String MEDIUMTEXT = "MEDIUMTEXT";
	public static final String LONGTEXT = "LONGTEXT";
	public static final String YEAR = "YEAR";
	public static final String TIME = "TIME";
	public static final String DATE = "DATE";
	public static final String DATETIME = "DATETIME";
	public static final String TIMESTAMP = "TIMESTAMP";
	
	public static final String DB_ROW_ID = "DB_ROW_ID";
	public static final String DB_TRX_ID = "DB_TRX_ID";
	public static final String DB_ROLL_PTR = "DB_ROLL_PTR";
	
	public static int getFixedLength(String type) {
		if (Objects.isNull(type)) {
			throw new IllegalArgumentException("column type is null.");
		}
		type = type.trim().toUpperCase();
		switch (type) {
		case TINYINT  : case UNSIGNED_TINYINT  : return 1;
		case SMALLINT : case UNSIGNED_SMALLINT : return 2;
		case MEDIUMINT: case UNSIGNED_MEDIUMINT: return 3;
		case INT      : case UNSIGNED_INT      : return 4;
		case BIGINT   : case UNSIGNED_BIGINT   : return 8;
		
		//FLOAT(p)	4 bytes if 0 <= p <= 24, 8 bytes if 25 <= p <= 53
		case FLOAT    : return 4;
		case DOUBLE   : return 8;
		
		////Date and Time Data Types
		case DATE       : return 3;
		case TIME       : return 3;
		case DATETIME   : return 8;
		case TIMESTAMP  : return 4;
		case YEAR       : return 1;
		
		//// Pseudo Column Types;
		case DB_ROW_ID  : return 6;
		case DB_TRX_ID  : return 6;
		case DB_ROLL_PTR: return 7;
		
		default:
			throw new IllegalArgumentException("unknown fixed-length column type: " + type);
		}
	}
	
	
	/**
	 * Get the storage requirements of DECIMAL(precision, scale).
	 * 
	 * <p>Return the storage requirements of integer and fractional parts. 
	 * <br>
	 * int[0] = integer parts bytes;<br>
	 * int[1] = fractional parts bytes;
	 * 
	 * @param precision the precision of DECIMAL
	 * @param scale the scale of DECIMAL
	 * @return the storage requirements for the integer and fractional parts
	 */
	public static int[] getDigitsBytes(int precision, int scale) {
		if (precision < 1 && scale < 0 && precision < scale) {
			throw new IllegalArgumentException("illegal precision, scale (" + precision + ", " + scale
					+ "), the requirements: precision >= 1 and scale >= 0 and precision >= scale .");
		}
		
		final int digits_pre_4bytes = 9; // 4 bytes store 9 digits;
		int integerDigits = precision - scale;
		int integerQuotient = integerDigits / digits_pre_4bytes;
		int integerLeftover = integerDigits % digits_pre_4bytes;
		int integerLeftoverBytes = getLeftoverBytes(integerLeftover);
		int integerBytes = 4 * integerQuotient + integerLeftoverBytes;

		int fractionQuotient = scale / digits_pre_4bytes;
		int fractionLeftover = scale % digits_pre_4bytes;
		int fractionLeftoverBytes = getLeftoverBytes(fractionLeftover);
		int fractionBytes = 4 * fractionQuotient + fractionLeftoverBytes;
		return new int[] { integerBytes, fractionBytes };
	}

	private static int getLeftoverBytes(int leftover) {
		int bytes = 0;
		if (leftover >= 1 && leftover <= 2) {
			bytes = 1;
		} else if (leftover >= 3 && leftover <= 4) {
			bytes = 2;
		} else if (leftover >= 5 && leftover <= 6) {
			bytes = 3;
		} else if (leftover >= 7 && leftover <= 9) {
			bytes = 4;
		}
		return bytes;
	}
	
	public static BigInteger getIntValue(byte[] bytes) {
		boolean negative = (bytes[0] & 0x80) != 0x80;
		bytes[0] ^= 0x80;
		if (negative) {
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] ^= 0xFF;
			}
		}
		BigInteger value = new BigInteger(bytes);
		if (negative) {
			return value.negate().subtract(BigInteger.ONE);
		}
		return value;
	}

	public static BigInteger getUIntValue(byte[] bytes) {
		BigInteger value = new BigInteger(bytes);
		BigInteger maxUnsigned = BigInteger.valueOf(2).pow(bytes.length * 8).subtract(BigInteger.ONE);
		return value.and(maxUnsigned);
	}
	
	public static BigDecimal getDecimalValue(byte[] bytes, int precision, int scale) {
		int[] digitsBytes = getDigitsBytes(precision, scale);
		int integerBytes = digitsBytes[0];
		int fractionalBytes = digitsBytes[1];

		boolean negative = (bytes[0] & 0x80) == 0x80;
		bytes[0] ^= 0x80;
		negative = !negative;
		if (negative) {
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] ^= 0xFF;
			}
		}
		
		int pos = 0;
		byte[] integerRaw = Arrays.copyOfRange(bytes, pos, pos += integerBytes);
		byte[] fractionalRaw = Arrays.copyOfRange(bytes, pos, pos += fractionalBytes);
		BigInteger integer = new BigInteger(integerRaw);
		// value = integer + fractional/10^scale
		BigDecimal decimal = new BigDecimal(integer);
		if (fractionalRaw.length > 0) {
			BigDecimal fractional = new BigDecimal(new BigInteger(fractionalRaw))
					.divide(new BigDecimal(new BigInteger("10").pow(scale)));
			decimal = decimal.add(fractional);
		}
		if (negative) {
			decimal = decimal.negate();
		}
		return decimal;
	}
	
	public static float getFloatValue(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		return buffer.getFloat();
	}

	public static double getDoubleValue(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		return buffer.getDouble();
	}

	public static LocalDateTime getTimestampValue(byte[] bytes) {
		long timestamp = getUInt32(bytes);
		return Instant.ofEpochSecond(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
		// return new Date(timestamp * 1000L);
	}
	
	/**
	 * DATETIME encoding for non-fractional part:
	 * 
	 * <pre>
	 * Bit(s) Content
	 * ------ ------------------------------------------------------------------------
	 *  1     sign bit, the sign bit is always 1. a value of 0 (negative) is reserved.
	 *  17    year*13 + month (year 0-9999, month 0-12)
	 *  5     day (0-31) 
	 *  5     hour (0-23) 
	 *  6     minute (0-59) 
	 *  6     second (0-59)
	 * </pre>
	 * 
	 * @param bytes
	 * @return
	 */
	public static LocalDateTime getDateTimeV2(byte[] bytes) {
		byte[] newbytes = getReverse(bytes);
		BitSet bits = BitSet.valueOf(newbytes);
		int yearMonth = (int) getLong(bits.get(46, 62));
		int year = yearMonth / 13;
		int month = yearMonth % 13;
		int day = (int) getLong(bits.get(41, 46));
		int hour = (int) getLong(bits.get(36, 41));
		int minute = (int) getLong(bits.get(30, 36));
		int second = (int) getLong(bits.get(24, 30));
		return LocalDateTime.of(year, month, day, hour, minute, second);
	}
	

	
}
