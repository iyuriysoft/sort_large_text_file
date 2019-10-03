package com.mergesort;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestSortFile {
  
  private static final String[] givenArr = { "xxcv", "xxcv", "qwerty", "aabb", "oooo", "aabb",
      "bbaa", "oooo", "bbaa", "zzxc", "oooo", "aaaaaa", "ooooo", "bbbaaa", "zzxc", "aabb", "bbaa",
      "zzz", "bbaa", "zzxc", };
  private static final String[] expectedArr = { "aaaaaa", "aabb", "aabb", "aabb", "bbaa", "bbaa",
      "bbaa", "bbaa", "bbbaaa", "oooo", "oooo", "oooo", "ooooo", "qwerty", "xxcv", "xxcv", "zzxc",
      "zzxc", "zzxc", "zzz", };
  private static final String INPUT_FILE = "input_test.txt";
  private static StartSort obj;

  @BeforeClass
  public static void initAll() throws IOException {
    Files.write(Paths.get(INPUT_FILE),
        (Iterable<String>) Arrays.asList(givenArr).stream()::iterator);
    obj = new StartSort("", INPUT_FILE, 3, 8);
  }

  @Before
  public void init() throws IOException {
  }

  @Test
  public void test1_SplitAndMerge() throws Exception {
    long start1 = 5, start2 = 9, end1 = 9, end2 = 13;
    int range1 = (int) (end1 - start1);
    int range2 = (int) (end2 - start2);
    String out_1 = "out_test1", out_2 = "out_test2", out = "out_test";
    
    // test split
    
    new ThreadSplitTextFile(out_1, INPUT_FILE, start1, end1).call();
    String[] result1 = Files.readAllLines(Paths.get(out_1)).toArray(new String[range1]);
    String[] expected1 = {"aabb", "bbaa", "bbaa", "oooo"};
    assertArrayEquals(expected1, result1);
    
    new ThreadSplitTextFile(out_2, INPUT_FILE, start2, end2).call();
    String[] result2 = Files.readAllLines(Paths.get(out_2)).toArray(new String[range2]);
    String[] expected2 = {"aaaaaa", "oooo", "ooooo", "zzxc"};
    assertArrayEquals(expected2, result2);

    // test merge
    
    new ThreadMergeTextFiles(out_1, out_2, out).call();
    String[] result = Files.readAllLines(Paths.get(out)).toArray(new String[range1 + range2]);
    String[] expected = {"aaaaaa", "aabb", "bbaa", "bbaa", "oooo", "oooo", "ooooo", "zzxc"};
    assertArrayEquals(expected, result);
  }

  @Test
  public void test2_SplitAndMergeInPool() throws IOException, InterruptedException, ExecutionException {
    List<String> ar = obj.splitLargeFile(givenArr.length);
    assertEquals(7, ar.size());
    String[] result = Files.readAllLines(Paths.get("out_6")).toArray(new String[2]);
    String[] expected = { "bbaa", "zzxc" };
    assertArrayEquals(expected, result);
    String[] result2 = Files.readAllLines(Paths.get("out_5")).toArray(new String[3]);
    String[] expected2 = { "aabb", "bbaa", "zzz" };
    assertArrayEquals(expected2, result2);

    String nameOut = obj.mergeSortedFiles(ar);
    String[] resultAll = Files.readAllLines(Paths.get(nameOut))
        .toArray(new String[givenArr.length]);
    assertArrayEquals(expectedArr, resultAll);
    Files.delete(Paths.get(nameOut));
  }

  @Test
  public void test3_FullProcessInPool() throws IOException, InterruptedException, ExecutionException {
    String name = obj.processSort();
    String[] result = Files.readAllLines(Paths.get(name)).toArray(new String[givenArr.length]);
    assertArrayEquals(expectedArr, result);
    Files.delete(Paths.get(name));
  }

  @After
  public void tearDown() {
  }

  @AfterClass
  public static void tearDownAll() {
    new File(INPUT_FILE).delete();
  }

}
