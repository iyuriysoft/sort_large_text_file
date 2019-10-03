package com.mergesort;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

public class CreateTestFile {
  
  public static void main(String[] args) throws Exception {
    String inputFileName = "input.txt";
    long lCount = 10_000_000L;
    int iWordLen = 6;
    if (args.length == 3) {
      inputFileName = args[0];
      lCount = Long.parseLong(args[1]);
      iWordLen = Integer.parseInt(args[2]);
    }
    createTestData(inputFileName, lCount, iWordLen);
    System.out.print("ok!");
  }
  
  public static void createTestData(String name, long cnt, int wordLen) throws FileNotFoundException, IOException {
    try (OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(name, false))) {
      for (int i = 0; i < cnt; i++) {
        out.append(generatingRandomString(wordLen) + System.lineSeparator());
      }
    }
  }

  public static String generatingRandomString(int wordLen) {
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    int targetStringLength = wordLen;
    Random random = new Random();
    StringBuilder buffer = new StringBuilder(targetStringLength);
    for (int i = 0; i < targetStringLength; i++) {
      int randomLimitedInt = leftLimit + (int) (random.nextFloat() * (rightLimit - leftLimit + 1));
      buffer.append((char) randomLimitedInt);
    }
    return buffer.toString();
  }

}
