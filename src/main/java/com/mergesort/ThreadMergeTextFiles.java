package com.mergesort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

/**
 * Thread to merge two sorted files
 *
 */
public class ThreadMergeTextFiles implements Callable<Integer> {
  private String nameIn1, nameIn2, nameOut;

  /**
   * 
   * @param fileNameIn1:
   *          first input file name
   * @param fileNameIn2:
   *          second input file name
   * @param fileNameOut:
   *          output file name
   */
  public ThreadMergeTextFiles(String fileNameIn1, String fileNameIn2, String fileNameOut) {
    this.nameIn1 = fileNameIn1;
    this.nameIn2 = fileNameIn2;
    this.nameOut = fileNameOut;
  }

  @Override
  public Integer call() throws Exception {
    String st = Thread.currentThread().getName();
    System.out.println(
        String.format("%s Merge start; (%s) (%s) -> %s", st, nameIn1, nameIn2, nameOut));
    try (BufferedReader br1 = new BufferedReader(new FileReader(nameIn1));
        BufferedReader br2 = new BufferedReader(new FileReader(nameIn2));
        BufferedWriter out = new BufferedWriter(new FileWriter(nameOut))) {
      String line1 = br1.readLine();
      String line2 = br2.readLine();
      while (line1 != null || line2 != null) {
        if (line1 == null || (line2 != null && line1.compareTo(line2) > 0)) {
          out.write(line2 + System.lineSeparator());
          line2 = br2.readLine();
        } else {
          out.write(line1 + System.lineSeparator());
          line1 = br1.readLine();
        }
      }
    }
    Files.delete(Paths.get(nameIn1));
    Files.delete(Paths.get(nameIn2));
    System.out.println(String.format("%s Merge end", st));
    return null;
  }
}
