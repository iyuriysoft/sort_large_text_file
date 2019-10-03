package com.mergesort;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Sort large file
 *
 */
public class StartSort {
  private static final String TEMP_NAME = "out_";
  private String szDir;
  private String szInputFile;
  private int iThreads;
  private long linesPart;

  public static void main(String[] args) throws Exception {
    String inputFileName = "input.txt";
    long linesPart = 25_000L;
    int iThreads = 8;
    if (args.length == 3) {
      inputFileName = args[0];
      linesPart = Long.parseLong(args[1]);
      iThreads = Integer.parseInt(args[2]);
    }
    long startTime = System.nanoTime();
    StartSort sortLarge = new StartSort("", inputFileName, linesPart, iThreads);
    String sortedFile = sortLarge.processSort();
    System.out.println("Final sorted file: " + sortedFile);
    System.out.println((System.nanoTime() - startTime) / 1e9);
  }

  /**
   * 
   * @param name:
   *          full file name
   * @return count of the lines
   * @throws IOException
   */
  public static long getLineCount(String name) throws IOException {
    try (Stream<String> sr = Files.newBufferedReader(Paths.get(name)).lines()) {
      return sr.count();
    }
  }

  /**
   * 
   * @param szDir:
   *          working path
   * @param szInputFile:
   *          input file name
   * @param linesPart:
   *          count of the lines for splitted files
   * @param iThreads:
   *          max threads count
   */
  public StartSort(String szDir, String szInputFile, long linesPart, int iThreads) {
    this.szDir = szDir;
    this.szInputFile = szInputFile;
    this.linesPart = linesPart;
    this.iThreads = iThreads;
  }

  /**
   * 
   * @return output file name
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws IOException
   */
  public String processSort() throws InterruptedException, ExecutionException, IOException {
    long linesAll = getLineCount(szDir + szInputFile);

    System.out.println(
        String.format("all:%d range:%d parts:%d", linesAll, linesPart, linesAll / linesPart));

    // 1. Split large file to small sorted files
    System.out.println("\nStart Split\n");
    List<String> arFileNames = this.splitLargeFile(linesAll);

    // 2. Merge all small sorted files
    System.out.println("\nStart Merge\n");
    return this.mergeSortedFiles(arFileNames);
  }

  /**
   * 
   * @param linesAll:
   *          count of the all lines in large file
   * @param linesPart:
   *          count of the lines for the small files (after split)
   * @return full file names of the splitted files
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public List<String> splitLargeFile(long linesAll)
      throws InterruptedException, ExecutionException {

    // Prepare threads
    List<Callable<Integer>> arThreads = new ArrayList<Callable<Integer>>();
    List<String> arNames = new ArrayList<String>();
    long endLine = 0;
    for (long startLine = 0, i = 0; startLine < linesAll; startLine += linesPart, i++) {
      endLine += linesPart;
      String s = szDir + TEMP_NAME + i;
      arNames.add(s);
      arThreads.add(new ThreadSplitTextFile(s, szDir + szInputFile, startLine, endLine));
    }

    // Run threads
    ExecutorService pool = Executors.newFixedThreadPool(iThreads);
    pool.invokeAll(arThreads);
    pool.shutdown();
    return arNames;
  }

  /**
   * 
   * @param arNames:
   *          full file names of the splitted files
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public String mergeSortedFiles(List<String> arFileNames)
      throws InterruptedException, ExecutionException {

    // prepare threads
    List<String> arNames = new ArrayList<>(arFileNames);
    int treeHeight = (int) (Math.log(arNames.size()) / Math.log(2));
    int i = 0;
    while (++i < treeHeight + 2 && arNames.size() > 1) {
      List<Callable<Integer>> arThreads = new ArrayList<Callable<Integer>>();
      List<String> arNamesNextLevel = new ArrayList<>();

      while (arNames.size() > 1) {
        String tmp1 = arNames.get(0);
        arNames.remove(0);
        String tmp2 = arNames.get(0);
        arNames.remove(0);
        String nameOut = szDir + TEMP_NAME + (arNames.size() + i * 10000);
        arNamesNextLevel.add(nameOut);
        arThreads.add(new ThreadMergeTextFiles(tmp1, tmp2, nameOut));
      }

      // Run thread pool
      ExecutorService pool = Executors.newFixedThreadPool(iThreads);
      pool.invokeAll(arThreads);
      pool.shutdown();
      // now, arNames should have 0 or 1 item
      arNames.addAll(arNamesNextLevel);
      System.out.println(" level " + i);
    }
    return arNames.get(0);
  }
}
