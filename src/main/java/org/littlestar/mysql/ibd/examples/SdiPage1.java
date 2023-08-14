package org.littlestar.mysql.ibd.examples;

import java.util.List;

import org.littlestar.mysql.common.ParserHelper;
import org.littlestar.mysql.ibd.page.SdiPage;
import org.littlestar.mysql.ibd.page.SdiPage.SdiRecord;
import org.littlestar.mysql.ibd.parser.IbdFileParser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class SdiPage1 {
	public static void main(String[] args) throws Exception {
		String fileName = "D:\\Data\\mysql\\8.0.18\\data\\sakila\\film.ibd";
		try (IbdFileParser parser = new IbdFileParser(fileName)) {
			SdiPage page = (SdiPage) parser.getPage(3);
			List<SdiRecord> records = page.getUserRecords();
			StringBuilder buff = new StringBuilder();
			int rows = 0;
			for(SdiRecord record: records) {
				byte[] unZipData = record.getUnZipDataRaw();
				byte[] zipData = record.getZipDataRaw();
				String json = toPretty(new String(unZipData));
				String format = "%11s : ";
				buff.append("\n*************************** ").append(++rows).append(". row ***************************\n")
					.append(String.format(format, "type")).append(record.getType()).append("\n")
					.append(String.format(format, "id")).append(record.getId()).append("\n")
					.append(String.format(format, "DB_TRX_ID")).append(ParserHelper.toHexString(record.getTrxIdRaw())).append("\n")
					.append(String.format(format, "DB_ROLL_PTR")).append(ParserHelper.toHexString(record.getRollPrtRaw())).append("\n")
					.append(String.format(format, "unzip_len")).append(record.getUncompressedLen()).append(" , actual : ").append(unZipData.length).append("\n")
					.append(String.format(format, "zip_len")).append(record.getCompressedLen()).append(" , actual : ").append(zipData.length).append("\n")
					.append("\n*************************** Content ***************************\n").append(json).append("\n");
			}
			System.out.println(buff);
		}
	}

	public static String toPretty(String jsonString) {
		JsonElement jsonElement = JsonParser.parseString(jsonString);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String prettyJson = gson.toJson(jsonElement);
		return prettyJson;
	}
}
