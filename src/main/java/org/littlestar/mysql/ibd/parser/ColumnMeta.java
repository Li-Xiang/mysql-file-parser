package org.littlestar.mysql.ibd.parser;

import static org.littlestar.mysql.ibd.parser.ColumnType.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ColumnMeta implements Comparable<ColumnMeta> {
	private int pos;
	private String name;
	private String type;
	private boolean isNullable = false;
	private boolean isVariableLength = false;
	private boolean isInternal = false;
	private int precision = -1;
	private int scale = -1;
	/** ENUM/SET data type member map<index, value> */
	private final HashMap<Integer, String> memberMap = new HashMap<Integer, String>(); 
	/**
	 * Column (maximum) length in bytes.
	 */
	private int length = -1;

	public int getPos() {
		return pos;
	}

	public ColumnMeta setPos(int pos) {
		this.pos = pos;
		return this;
	}

	public String getName() {
		return name;
	}

	public ColumnMeta setName(String name) {
		this.name = name;
		return this;
	}

	public String getType() {
		return type;
	}

	public ColumnMeta setType(String type) {
		if (Objects.isNull(type)) {
			throw new IllegalArgumentException("data byte is null. ");
		}
		this.type = type.trim().toUpperCase();
		return this;
	}

	public int getLength() {
		return length;
	}

	/**
	 * Column defined (maximum, if variable-length column) length in bytes.
	 * 
	 * @param length
	 */
	public ColumnMeta setLength(int length) {
		this.length = length;
		return this;
	}
	
	public int getPrecision() {
		return precision;
	}

	public ColumnMeta setPrecision(int precision) {
		this.precision = precision;
		return this;
	}

	public int getScale() {
		return scale;
	}

	public ColumnMeta setScale(int scale) {
		this.scale = scale;
		return this;
	}
	
	/**
	 * MySQL Enum data type index start with 1.
	 * 
	 * @param members
	 * @return
	 */
	public ColumnMeta setEnumMembers(String... members) {
		for (int i = 0; i < members.length; i++) {
			memberMap.put(i + 1, members[i]);
		}
		return this;
	}
	
	public ColumnMeta setEnumMembers(Map<Integer, String> members) {
		memberMap.putAll(members);
		return this;
	}
	
	public Map<Integer, String> getEnumMembers() {
		return this.memberMap;
	}

	public String getEnumMember(int index) {
		return memberMap.get(index);
	}
	
	public boolean isNullable() {
		return isNullable;
	}

	public ColumnMeta setNullable(boolean isNullable) {
		this.isNullable = isNullable;
		return this;
	}
	
	public boolean isVariableLength() {
		return isVariableLength;
	}

	public ColumnMeta setVariable(boolean isVariableLength) {
		this.isVariableLength = isVariableLength;
		return this;
	}
	
	public boolean isInternal() {
		return isInternal;
	}

	public ColumnMeta setInternal(boolean isInternal) {
		this.isInternal = isInternal;
		return this;
	}
	
	@Override
	public int compareTo(ColumnMeta col2) {
		Integer col1Pos = getPos();
		return col1Pos.compareTo(col2.getPos());
	}
	
	public static ColumnMeta newDecimalColumnMeta(int pos, String name, boolean nullable, int precision, int scale ) {
		int[] bytes = ColumnType.getDigitsBytes(precision, scale);
		int length = bytes[0] + bytes[1];
		return new ColumnMeta()
				.setType(DECIMAL)
				.setPos(pos)
				.setName(name)
				.setLength(length)
				.setNullable(nullable)
				.setVariable(false)
				.setPrecision(precision)
				.setScale(scale);
	}
	
	public static ColumnMeta newColumnMeta(String type, int pos, String name, int bytes, boolean nullable,
			boolean variable) {
		type = type.trim().toUpperCase();
		return new ColumnMeta()
				.setType(type)
				.setPos(pos)
				.setName(name)
				.setLength(bytes)
				.setNullable(nullable)
				.setVariable(variable);
	}
	
	/**
	 * https://dev.mysql.com/doc/refman/8.0/en/innodb-row-format.html#innodb-compact-row-format-characteristics
	 * Internally, for variable-length character sets such as utf8mb3 and utf8mb4, InnoDB attempts to store CHAR(N) in N bytes by trimming trailing spaces.
	 * 
	 * @return
	 */
	public static ColumnMeta newCharColumnMeta(int pos, String name, int bytes, boolean nullable, boolean variable) {
		return new ColumnMeta().setType(CHAR)
				.setPos(pos)
				.setName(name)
				.setLength(bytes)
				.setNullable(nullable)
				.setVariable(variable);
	}
	
	public static ColumnMeta newEnumColumnMeta(int pos, String name, boolean nullable, String... members) {
		int bytes = 1;
		if (members.length > 255) {
			bytes = 2;
		}
		return new ColumnMeta().setType(ENUM)
				.setPos(pos)
				.setName(name)
				.setLength(bytes)
				.setNullable(nullable)
				.setVariable(false)
				.setEnumMembers(members);
	}
	
	public static ColumnMeta newSetColumnMeta(int pos, String name, boolean nullable, String... members) {
		final int setSize = members.length;
		int bytes = (setSize + 7) / 8;
		if (bytes > 8)
			throw new RuntimeException("");
		return new ColumnMeta().setType(SET)
				.setPos(pos)
				.setName(name)
				.setLength(bytes)
				.setNullable(nullable)
				.setVariable(false)
				.setEnumMembers(members);
	}
	
	public static ColumnMeta newFixLengthColumnMeta(String type, int pos, String name, boolean nullable) {
		type = type.trim().toUpperCase();
		int bytes = getFixedLength(type);
		return newColumnMeta(type, pos, name, bytes, nullable, false);
	}

	public static ColumnMeta newVariableColumnMeta(String type, int pos, String name, boolean nullable, int maxBytes) {
		return newColumnMeta(type, pos, name, maxBytes, nullable, true);
	}
	
	/** 
	 * create a new variable-length column meta instance, which column max length less or equal 255 bytes.
	 * 
	 * @return small variable-length column meta instance.
	 */
	public static ColumnMeta newSmallVariableLengthColumnMeta(String type, int pos, String name, boolean nullable) {
		return new ColumnMeta()
				.setName(name)
				.setPos(pos)
				.setType(type)
				.setLength(255)
				.setVariable(true)
				.setNullable(nullable);
	}
	
	public static ColumnMeta newRowIdColumnMeta(int pos) {
		int len = getFixedLength(DB_ROW_ID);
		return new ColumnMeta()
				.setName(DB_ROW_ID)
				.setPos(pos)
				.setType(DB_ROW_ID)
				.setLength(len)
				.setNullable(false)
				.setVariable(false)
				.setInternal(true);
	}
	
	public static ColumnMeta newTrxIdColumnMeta(int pos) {
		int len = getFixedLength(DB_TRX_ID);
		return new ColumnMeta()
				.setName(DB_TRX_ID)
				.setPos(pos)
				.setType(DB_TRX_ID)
				.setLength(len)
				.setNullable(false)
				.setVariable(false)
				.setInternal(true);
	}
	
	public static ColumnMeta newRollPtrColumnMeta(int pos) {
		int len = getFixedLength(DB_ROLL_PTR);
		return new ColumnMeta()
				.setName(DB_ROLL_PTR)
				.setPos(pos)
				.setType(DB_ROLL_PTR)
				.setLength(len)
				.setNullable(false)
				.setVariable(false)
				.setInternal(true);
	}
	
}