# Spark_HW

assignment of Big Data System course in Taiwan, NDHU

----

## PageRank

Implement the `PageRank` algo. with Spark and provide suitable input to test it


## GPA

Write a Spark program to compute the `GPA` of each student and the `average grade` of each course

Given a `file` with `student grade reports` in the format:

    StudentID CourseID1 Grade1 Course2 Grade2 ...
    
and a  `file` of `class credits` in the format:

    CourseID1 #credit1 CourseID2 #credit2 ...

## Inverted Index

Write a Spark program to compute the inverted index of a set of documents. 
More specifically, 
given `a set of (DocumentID, text) pairs`,
output `a list of (word, (doc1, doc2,…)) pairs`.

## Frequent Item Set
Given a `text file` of `purchase records` and a threshold `θ`, write a Spark program to find all `sets` of `frequent items`
that are purchased together. Each line of the input is a `transaction` of the format:

    <tid> item1 item2 ...

where <tid> is the transaction ID and itemi are the purchased items (all represented by integer IDs).
A set of items is considered `frequent` if it appears in `at least θ` transactions. 

Keep in mind that `purchase order is irrelevant`. {A, B} is the same as {B, A}. If a set appears
in a transaction, it is only `counted once` no matter how many times it appears in that transaction.


