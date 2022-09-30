import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import java.util.ArrayList;

class JavaRegexTest
{
  public static final String str_pattern = "^(\\S+) \\S+ \\S+ \\[[\\w:/]+\\s[+\\-]\\d{4}\\] \"\\S+ \\S+\\s*\\S*\\s*\" \\d{3} \\S+";
  public static void main(String args[]) 
  { 
    // basic verify arguments
    if(args.length != 1) {
      System.out.println("Usage: java tester <inputfile>");
      return;
    }
    
    // open and read input into memory
    ArrayList<String> input = new ArrayList<>();
    try {
      Scanner scanner = new Scanner(new File(args[0]));
      while(scanner.hasNextLine()) {
        input.add(scanner.nextLine());
      }
      scanner.close();
    } catch(FileNotFoundException e) {
      e.printStackTrace();
    }

    // compile regex
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(str_pattern);

    // allocate output space
    int num_success = 0, num_failed = 0;
    int[] res = new int[2*input.size()];
    
    // loop over input and run match
    long start = System.nanoTime();
    for(int i = 0; i < input.size(); i++) {
      // run match
      java.util.regex.Matcher matcher = pattern.matcher(input.get(i));

      // record output
      if (matcher.find()) {
        java.util.regex.MatchResult matchResult = matcher.toMatchResult();
        //if (matchResult.group(1) != null) {
          res[2*num_success] = matchResult.start(1);
          res[2*num_success + 1] = matchResult.end(1) - matchResult.start(1);
        //}
        num_success++;
      } else {
        num_failed++;
      }
    }
    long stop = System.nanoTime();
    long duration = stop - start;
    
    // output stats
    System.out.format("{\"executionTime\": %f, \"num_success\":%d, \"num_failed\":%d}%n", duration/((double)1000000000), num_success, num_failed);
  }
}
