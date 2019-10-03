package com.mergesort;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * Thread to split large file on small sorted files
 *
 */
public class ThreadSplitTextFile implements Callable<Integer> {
  private String nameOut, nameIn;
  private long start, range;

  /**
   * 
   * @param fileNameOut: output file name
   * @param fileNameIn: input file name
   * @param startLine: line number (included)
   * @param endLine: line number
   */
  public ThreadSplitTextFile(String fileNameOut, String fileNameIn, long startLine, long endLine) {
    this.nameOut = fileNameOut;
    this.nameIn = fileNameIn;
    this.start = startLine;
    this.range = endLine - startLine;
  }

  @Override
  public Integer call() throws Exception {
    String st = Thread.currentThread().getName();
    System.out.println(String.format("%s Split begin; start:%d  count:%d", st, start, range));
    try (Stream<String> sr = Files.newBufferedReader(Paths.get(nameIn)).lines();) {
      Stream<String> stream = sr.skip(start).limit(range).sorted(Comparator.naturalOrder());
      Files.write(Paths.get(nameOut), (Iterable<String>) stream::iterator);
    }
    System.out.println(String.format("%s Split end; sort done", st));
    return null;
  }
}
