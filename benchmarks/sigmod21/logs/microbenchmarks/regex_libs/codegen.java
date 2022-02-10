/*
 * Output from Spark when the following is run:
 *  val df = spark.read.text("inputfile.txt").select(regexp_extract($"value", "^(\\S+) \\S+ \\S+ \\[[\\w:/]+\\s[+\\-]\\d{4}\\] \"\\S+ \\S+\\s*\\S*\\s*\" \\d{3} \\S+", 1).alias("ip"))
 *  df.queryExecution.debug.codegen
 * Used as reference to write JavaRegeTest.java
 */
public Object generate(Object[] references) {
  return new GeneratedIteratorForCodegenStage1(references);
}

// codegenStageId=1
final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] project_mutableStateArray_2 = new org.apache.spark.sql.catalyst.expressions.codegen.U
nsafeRowWriter[1];
  private java.util.regex.Pattern[] project_mutableStateArray_1 = new java.util.regex.Pattern[1];
  private scala.collection.Iterator[] scan_mutableStateArray_0 = new scala.collection.Iterator[1];
  private UTF8String[] project_mutableStateArray_0 = new UTF8String[1];

  public GeneratedIteratorForCodegenStage1(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    scan_mutableStateArray_0[0] = inputs[0];

    project_mutableStateArray_2[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);

  }

  protected void processNext() throws java.io.IOException {
    while (scan_mutableStateArray_0[0].hasNext()) {
      InternalRow scan_row_0 = (InternalRow) scan_mutableStateArray_0[0].next();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
      boolean project_isNull_0 = true;
      UTF8String project_value_0 = null;
      boolean scan_isNull_0 = scan_row_0.isNullAt(0);
      UTF8String scan_value_0 = scan_isNull_0 ?
      null : (scan_row_0.getUTF8String(0));
      if (!scan_isNull_0) {
        project_isNull_0 = false; // resultCode could change nullability.

        if (!((UTF8String) references[1] /* literal */).equals(project_mutableStateArray_0[0])) {
          // regex value changed
          project_mutableStateArray_0[0] = ((UTF8String) references[1] /* literal */).clone();
          project_mutableStateArray_1[0] = java.util.regex.Pattern.compile(project_mutableStateArray_0[0].toString());
        }
        java.util.regex.Matcher project_matcher_0 =
        project_mutableStateArray_1[0].matcher(scan_value_0.toString());
        if (project_matcher_0.find()) {
          java.util.regex.MatchResult project_matchResult_0 = project_matcher_0.toMatchResult();
          if (project_matchResult_0.group(1) == null) {
            project_value_0 = UTF8String.EMPTY_UTF8;
          } else {
            project_value_0 = UTF8String.fromString(project_matchResult_0.group(1));
          }
          project_isNull_0 = false;
        } else {
          project_value_0 = UTF8String.EMPTY_UTF8;
          project_isNull_0 = false;
        }

      }
      project_mutableStateArray_2[0].reset();

      project_mutableStateArray_2[0].zeroOutNullBytes();

      if (project_isNull_0) {
        project_mutableStateArray_2[0].setNullAt(0);
      } else {
        project_mutableStateArray_2[0].write(0, project_value_0);
      }
      append((project_mutableStateArray_2[0].getRow()));
      if (shouldStop()) return;
    }
  }
}
