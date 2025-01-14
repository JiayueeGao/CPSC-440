# CPSC 440/540 Lab 3: Query Optimization

**Assigned:** Wednesday September 25, 2024

**Due:** Thursday October 10, 2024 by 11:59 PM ET

In this lab, you will implement a query optimizer on top of SimpleDB. The main
tasks include implementing a selectivity estimation framework and a cost-based
optimizer. You have freedom as to exactly what you implement, but we recommend
using something similar to the Selinger cost-based optimizer discussed in class
(Oct 1 paper).

The remainder of this document describes what is involved in adding optimizer
support and provides a basic outline of how you do so.

As with the previous lab, we recommend that you start as early as possible.


##  1. Getting started

You should begin with the code you submitted for Lab 2. (If you did not submit
code for Lab 2, or your solution didn't work properly, contact us to discuss
options.)

We have provided you with extra test cases as well as source code files for this
lab that are not in the original code distribution you received. We again
encourage you to develop your own test suite in addition to the ones we have
provided.

You will need to add these new files to your release. The easiest way to do this
is to change to your project directory (probably called `CPSC-440-FA24`) and
pull from the main GitHub repository:

```
$ cd CPSC-440-FA24
$ git pull upstream main
```

### 1.1. Implementation hints
We suggest exercises along this document to guide your implementation, but you
may find that a different order makes more sense for you. As before, we will
grade your assignment by looking at your code and verifying that you have passed
the test for the ant targets `test` and `systemtest`. See Section 3.4 for a
complete discussion of grading and the tests you will need to pass.

Here's a rough outline of one way you might proceed with this lab. More details
on these steps are given in Section 2 below.

*  Implement the methods in the `TableStats` class that allow
   it to estimate selectivities of filters and cost of scans, using histograms
   (skeleton provided for the `IntHistogram` class) or some other form of
   statistics of your devising.
*  Implement the methods in the `JoinOptimizer` class that
   allow it to estimate the cost and selectivities of joins.
*  Write the `orderJoins` method in `JoinOptimizer`. This method must produce
   an optimal ordering for a series of joins (likely using the Selinger algorithm),
   given statistics computed in the previous two steps.

##  2. Optimizer outline

Recall that the main idea of a cost-based optimizer is to:

*  Use statistics about tables to estimate "costs" of different
   query plans. Typically, the cost of a plan is related to the cardinalities of
   (number of tuples produced by) intermediate joins and selections, as well as the
   selectivity of filter and join predicates.
*  Use these statistics to order joins and selections in an
   optimal way, and to select the best implementation for join algorithms from
   amongst several alternatives.

In this lab, you will implement code to perform both of these functions.

The optimizer will be invoked from `simpledb/Parser.java`. You may wish to
review the [lab 2 parser
exercise](https://github.com/yale-systems/CPSC-440-FA24/blob/main/lab2.md#27-query-parser)
before starting this lab. Briefly, if you have a catalog file `catalog.txt`
describing your tables, you can run the parser by typing:
```bash
java -jar dist/simpledb.jar parser catalog.txt
```

When the `Parser` is invoked, it will compute statistics over all of the tables
(using statistics code you provide). When a query is issued, the parser will
convert the query into a logical plan representation and then call your query
optimizer to generate an optimal plan.

### 2.1 Overall Optimizer Structure
Before getting started with the implementation, you need to understand the
overall structure of the SimpleDB optimizer. The overall control flow of the
SimpleDB modules of the parser and optimizer is shown in Figure 1.

<p align="center">
<img width=400 src="lab3-controlflow.png"><br>
<i>Figure 1: Diagram illustrating classes, methods, and objects used in the parser</i>
</p>


The key at the bottom explains the symbols; you will implement the components
with double-borders. The classes and methods will be explained in more detail
in the text that follows (you may wish to refer back to this diagram), but the
basic operation is as follows:

1. `Parser.java` constructs a set of table statistics (stored in the
   `statsMap` container) when it is initialized. It then waits for a
   query to be input, and calls the method `parseQuery` on that query.
2. `parseQuery` first constructs a `LogicalPlan` that represents the parsed
   query.`parseQuery` then calls the method `physicalPlan` on the `LogicalPlan`
   instance it has constructed. The `physicalPlan` method returns a `DBIterator`
   object that can be used to actually run the query.

In the exercises to come, you will implement the methods that help
`physicalPlan` devise an optimal plan.

### 2.2. Statistics Estimation
Accurately estimating plan cost is quite tricky. In this lab, we will focus only
on the cost of sequences of joins and base table accesses. We won't worry about
access method selection (since we only have one access method, table scans) or
the costs of additional operators (like aggregates).

You are only required to consider left-deep plans for this lab. 

####  2.2.1 Overall Plan Cost

We will write join plans of the form `p=t1 join t2 join ... tn`, which signifies
a left deep join where t1 is the left-most join (deepest in the tree). Given a
plan like `p`, its cost can be expressed as:

```
scancost(t1) + scancost(t2) + joincost(t1 join t2) +
scancost(t3) + joincost((t1 join t2) join t3) +
...
```

Here, `scancost(t1)` is the I/O cost of scanning table t1, `joincost(t1,t2)` is
the CPU cost to join t1 to t2.  To make I/O and CPU cost comparable, typically a
constant scaling factor is used, e.g.:

```
cost(predicate application) = 1
cost(pageScan) = SCALING_FACTOR x cost(predicate application)
```

For this lab, you can ignore the effects of caching (e.g., assume that every
access to a table incurs the full cost of a scan.)
Therefore, `scancost(t1)` is simply the number of pages in `t1 x
SCALING_FACTOR`.

####  2.2.2 Join Cost

When using nested loops joins, recall that the cost of a join between two tables
t1 and t2 (where t1 is the outer) is simply:

```
joincost(t1 join t2) = scancost(t1) + ntups(t1) x scancost(t2) //IO cost
                       + ntups(t1) x ntups(t2)  //CPU cost
```

Here, `ntups(t1)` is the number of tuples in table t1.

####  2.2.3 Filter Selectivity

`ntups` can be directly computed for a base table by scanning that table.
Estimating `ntups` for a table with one or more selection predicates over it can
be trickier -- this is the *filter selectivity estimation* problem.  Here's one
approach that you might use, based on computing a histogram over the values in
the table:

*  Compute the minimum and maximum values for every attribute in the table (by
   scanning it once).
*  Construct a histogram for every attribute in the table. A simple
   approach is to use a fixed number of buckets *NumB*, with each bucket
   representing the number of records in a fixed range of the domain of the
   attribute of the histogram.  For example, if a field
   *f* ranges from 1 to 100, and there are 10 buckets, then bucket 1 might
   contain the count of the number of records between 1 and 10, bucket 2 a count
   of the number of records between 11 and 20, and so on.
*  Scan the table again, selecting out all of fields of all of the
   tuples and using them to populate the counts of the buckets in each
   histogram.
*  To estimate the selectivity of an equality expression, *f = const*, compute the
   bucket that contains value *const*. Suppose the width (range of values) of
   the bucket is *w*, the height (number of tuples) is *h*, and the number of
   tuples in the table is *ntups*.  Then, assuming values are uniformly
   distributed throughout the bucket, the selectivity of the expression is
   roughly *(h / w) / ntups*, since *(h/w)* represents the expected number of
   tuples in the bin with value *const*.
*  To estimate the selectivity of a range expression *f > const*, compute the
   bucket *b* that *const* is in, with width *w_b* and height
   *h_b*.  Then, *b* contains a fraction <nobr>*b_f = h_b / ntups* </nobr>of the
   total tuples.  Assuming tuples are uniformly distributed throughout *b*, the
   fraction *b_part* of *b* that is *> const* is <nobr>*(b_right - const) /
   w_b*</nobr>, where *b_right* is the right endpoint of
   *b*'s bucket.  Thus, bucket *b* contributes *(b_f  x
   b_part)* selectivity to the predicate.  In addition, buckets
   *b+1...NumB-1* contribute all of their
   selectivity (which can be computed using a formula similar to
   *b_f* above).  Summing the selectivity contributions of all the
   buckets will yield the overall selectivity of the expression. Figure 2
   illustrates this process.
*  Selectivity of expressions involving *less than* can be performed
   similar to the greater than case, looking at buckets down to 0.

<p align="center">
<img width=400 src="lab3-hist.png"><br>
<i>Figure 2: Diagram illustrating the histograms you will implement in Lab 5</i>
</p>

In the next two exercises, you will code to perform selectivity estimation of
joins and filters.

----------

**Exercise 1:  IntHistogram.java**

You will need to implement some way to record table statistics for selectivity
estimation.  We have provided a skeleton class, `IntHistogram` that will do
this.  Our intent is that you calculate histograms using the bucket-based method
described above, but you are free to use some other method so long as it
provides reasonable selectivity estimates.


We have provided a class `StringHistogram` that uses `IntHistogram` to compute
selecitivites for String predicates.  You may modify `StringHistogram` if you
want to implement a better estimator, though you should not need to in order to
complete this lab.

After completing this exercise, you should be able to pass the
`IntHistogramTest` unit test (you are not required to pass this test if you
choose not to implement histogram-based selectivity estimation). **If you
implement selectivity estimation some other way, please indicate so and
describe your method in your writeup.**

----------

**Exercise 2:  TableStats.java**

The class `TableStats` contains methods that compute the number of tuples and
pages in a table and that estimate the selectivity of predicates over the fields
of that table. The query parser we have created creates one instance of
`TableStats` per table, and passes these structures into your query optimizer
(which you will need in later exercises).

You should fill in the following methods and classes in `TableStats`:

*  Implement the `TableStats` constructor: Once you have
   implemented a method for tracking statistics such as histograms, you should
   implement the `TableStats` constructor, adding code to scan the table (possibly
   multiple times) to build the statistics you need.
*  Implement `estimateSelectivity(int field, Predicate.Op op, Field constant)`:
   Using your statistics (e.g., an `IntHistogram` or `StringHistogram` depending
   on the type of the field), estimate the selectivity of predicate `field op
   constant` on the table.
*  Implement `estimateScanCost()`: This method estimates the cost of
   sequentially scanning the file, given that the cost to read a page is
   `costPerPageIO`.  You can assume that there are no seeks and that no pages
   are in the buffer pool.  This method may use costs or sizes you computed in
   the constructor.
*  Implement `estimateTableCardinality(double selectivityFactor)`: This method
   returns the number of tuples in the relation, given that a predicate with
   selectivity selectivityFactor is applied. This method may use costs or sizes
   you computed in the constructor.

You may wish to modify the constructor of `TableStats.java` to, for example,
compute histograms over the fields as described above for purposes of
selectivity estimation.

After completing these tasks you should be able to pass the unit tests in
`TableStatsTest`.

----------

#### 2.2.4 Join Cardinality

Finally, observe that the cost for the join plan `p` above includes expressions
of the form `joincost((t1 join t2) join t3)`. To evaluate this expression, you
need some way to estimate the size (`ntups`) of `t1 join t2`.  This *join
cardinality estimation* problem is harder than the filter selectivity estimation
problem.  


While implementing your simple solution, you  should keep in mind the following:

* For equality joins, when one of the attributes is a primary key, the number
  of tuples produced by the join cannot be larger than the cardinality of the
  non-primary key attribute.
* For equality joins when there is no primary key, it's hard to say much about
  what the size of the output is -- it could be the size of the product of the
  cardinalities of the tables (if both tables have the same value for all
  tuples) -- or it could be 0.  It's fine to make up a simple heuristic (say,
  the size of the larger of the two tables).
* For range scans, it is similarly hard to say anything accurate about sizes.
  The size of the output should be proportional to the sizes of the inputs. It is
  fine to assume that a fixed fraction of the cross-product is emitted by range
  scans (say, 30%).  In general, the cost of a range join should be larger than
  the cost of a non-primary key equality join of two tables of the same size.

----------

**Exercise 3:  Join Cost Estimation**

The class `JoinOptimizer.java` includes all of the methods for ordering and
computing costs of joins.  In this exercise, you will write the methods for
estimating the selectivity and cost of a join, specifically:

* Implement `estimateJoinCost(LogicalJoinNode j, int card1, int card2, double
  cost1, double cost2)`: This method estimates the cost of join `j`, given that
  the left input is of cardinality `card1`, the right input of cardinality
  `card2`, that the cost to scan the left input is `cost1`, and that the cost to
  access the right input is `card2`. You can assume the join is an NL join, and
  apply the formula mentioned earlier.
* Implement `estimateJoinCardinality(LogicalJoinNode j, int card1, int card2,
  boolean t1pkey, boolean t2pkey)`: This method estimates the number of tuples
  output by join `j`, given that the left input is size `card1`, the right input
  is size `card2`, and the flags `t1pkey` and `t2pkey` that indicate whether the
  left and right (respectively) field is unique (a primary key).

After implementing these methods, you should be able to pass the unit tests
`estimateJoinCostTest` and `estimateJoinCardinality` in
`JoinOptimizerTest.java`.

----------

###  2.3 Join Ordering

Now that you have implemented methods for estimating costs, you will implement
the Selinger optimizer.  For these methods, joins are expressed as a list of
join nodes (e.g., predicates over two tables) as opposed to a list of relations
to join as described in class.

Translating the algorithm given in lecture to the join node list form mentioned
above, an outline in pseudocode would be:
```
1. j = set of join nodes
2. for (i in 1...|j|):
3.     for s in {all length i subsets of j}
4.       bestPlan = {}
5.       for s' in {all length d-1 subsets of s}
6.            subplan = optjoin(s')
7.            plan = best way to join (s-s') to subplan
8.            if (cost(plan) < cost(bestPlan))
9.               bestPlan = plan
10.      optjoin(s) = bestPlan
11. return optjoin(j)
```

To help you implement this algorithm, we have provided several classes and
methods to assist you.  First, the method `enumerateSubsets(List<T> v, int
size)` in `JoinOptimizer.java` will return a set of all of the subsets of `v` of
size `size`. This method is VERY inefficient for large sets; while it is **not** mandatory, 
you are encouraged to implement a more efficient enumerator, (hint: consider using an
in-place generation algorithm and a lazy iterator (or stream) interface to avoid
materializing the entire power set).

Second, we have provided the method:
```java
private CostCard computeCostAndCardOfSubplan(Map<String, TableStats> stats,
                                             Map<String, Double> filterSelectivities,
                                             LogicalJoinNode joinToRemove,
                                             Set<LogicalJoinNode> joinSet,
                                             double bestCostSoFar,
                                             PlanCache pc)
```

Given a subset of joins (`joinSet`), and a join to remove from this set
(`joinToRemove`), this method computes the best way to join `joinToRemove` to
`joinSet - {joinToRemove}`. It returns this best method in a `CostCard` object,
which includes the cost, cardinality, and best join ordering (as a list).
`computeCostAndCardOfSubplan` may return `null`, if no plan can be found
(because, for example, there is no left-deep join that is possible), or if the
cost of all plans is greater than the `bestCostSoFar` argument.  The method uses
a cache of previous joins called `pc` (`optjoin` in the psuedocode above) to
quickly lookup the fastest way to join `joinSet - {joinToRemove}`. The other
arguments (`stats` and `filterSelectivities`) are passed into the `orderJoins`
method that you must implement as a part of Exercise 4, and are explained below.
This method essentially performs lines 6 &ndash; 8 of the psuedocode described
earlier.

Third, we have provided the method:
```java
private void printJoins(List<LogicalJoinNode> js,
                        PlanCache pc,
                        Map<String, TableStats> stats,
                        Map<String, Double> selectivities)
```

This method can be used to display a graphical representation of a join plan
(when the "explain" flag is set via the "-explain" option to the optimizer, for
example).

Fourth, we have provided a class `PlanCache` that can be used to cache the best
way to join a subset of the joins considered so far in your implementation of
Selinger (an instance of this class is needed to use
`computeCostAndCardOfSubplan`).

----------

**Exercise 4:  Join Ordering**

In `JoinOptimizer.java`, implement the method:
```java
List<LogicalJoinNode> orderJoins(Map<String, TableStats> stats,
                                 Map<String, Double> filterSelectivities,
                                 boolean explain)
```

This method should operate on the `joins` class member, returning a new `List`
that specifies the order in which joins should be done. Item 0 of this list
indicates the left-most, bottom-most join in a left-deep plan. Adjacent joins in
the returned list should share at least one field to ensure the plan is
left-deep.  Here `stats` is an object that lets you find the `TableStats` for a
given table name that appears in the `FROM` list of the query.
`filterSelectivities` allows you to find the selectivity of any predicates over
a table; it is guaranteed to have one entry per table name in the `FROM` list.
Finally, `explain` specifies that you should output a representation of the join
order for informational purposes.

You may wish to use the helper methods and classes described above to assist in
your implementation. Roughly, your implementation should follow the psuedocode
above, looping through subset sizes, subsets, and sub-plans of subsets, calling
`computeCostAndCardOfSubplan` and building a `PlanCache` object that stores the
minimal-cost way to perform each subset join.

After implementing this method, you should be able to pass all the unit tests in
`JoinOptimizerTest`.  You should also pass the system test `QueryTest`.

----------

You have now completed this lab. Good work!

##  3. Logistics
You must submit your code (see below) as well as a short (2 pages, maximum)
writeup describing your approach.  This writeup should:

* Describe any design decisions you made, including methods for selectivity
  estimation and join ordering, and how you implemented them 
* Discuss and justify any changes you made to the API.
* Describe any missing or incomplete elements of your code.
* Describe how long you spent on the lab, and whether there was anything
  you found particularly difficult or confusing.

### 3.1. Honor Code

Although you are encouraged to discuss ideas with others, your programs are to 
be completed independently and must be your own. Whenever you obtain significant 
outside help (from other students, the TA, etc.) you should acknowledge this in 
your program write-up, e.g. “The idea for how to implement B-trees came from a 
discussion with Alice.”

Any violation of the University’s policies on Academic and Professional Integrity 
may result in serious penalties, which might range from failing an assignment, to 
failing a course, to being expelled from the program. 

### 3.2. Submitting your assignment

We will be using gradescope to autograde all programming assignments. You should
have all been invited to the class instance; if not, please email the TA for an
invite code. If you are still having trouble, let us know and we can help you
set up. You may submit your code multiple times before the deadline; we will use
the latest version as determined by gradescope. Place the write-up in a file
called `lab3-writeup.txt` with your submission. You also need to explicitly add
any other files you create, such as new `*.java` files.

The easiest way to submit to gradescope is with `.zip` files containing your
code. On Linux/MacOS, you can do so by running the following command:

```bash
$ zip -r submission.zip src/ lab3-writeup.txt
```

<a name="bugs"></a>

### 3.3. Submitting a bug

Please submit (friendly!) bug reports to the TA (amirmohammad.nazari@yale.edu). When you do, please try to
include:

* A description of the bug.
* A `.java` file we can drop in the `test/simpledb` directory, compile, and run.
* A `.txt` file with the data that reproduces the bug. We should be able to
  convert it to a `.dat` file using HeapFileEncoder.

<a name="grading"></a>

###  3.4 Grading
75% of your grade will be based on whether or not your code passes the system
test suite we will run over it. These tests will be a superset of the tests we
have provided. Before handing in your code, you should make sure it produces no
errors (passes all of the tests) from both `ant test` and `ant systemtest`.

If you are late for a deliverable, then you lose 10 points per day, up to 3 days 
late. After that you receive zero for that deliverable. Over the semester each 
student has a total of 1 free late day that you can use however you want for any 
of the assignments without penalty.

**Important:** Before testing, Gradescope will replace your `build.xml`
and the entire contents of the `test` directory with our version of these files.
This means you cannot change the format of `.dat` files! You should also be
careful changing our APIs. You should test that your code compiles the
unmodified tests.

You should get immediate feedback and error outputs for failed tests (if any)
from gradescope after submission. The score given will be your grade for the
autograded portion of the assignment. An additional 25% of your grade will be
based on the quality of your write-up and our subjective evaluation of your code.
This part will also be published on gradescope after we finish grading your
assignment.
