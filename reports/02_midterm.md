# DSPA 2019 - Midterm Report

Roland Schlaefli, 12-932-398 and Nicolas Kuechler, 14-712-129

## Challenges & Issues

> Challenges and issues faced so far and how you solved them or planning to solve them.

The main challenges so far were mostly related to documentation and examples for common but non-trivial patterns.

While the book and the documentation are great for an introduction to the most important basic building blocks of Flink we missed a collection of
design patterns for more complex pipelines possibly involving multiple windows. Additionally the transparent handling of watermarks / timestamps
lacks a detailed description of the effects which would in particular be helpful when working with Process Functions.

An additional challenge has been and will be the handling of the input formats and missing values, as we read the input data into Avro objects, which we then produce to Kafka for easy consumption.

Furthermore, while we were not sure about how to approach testing in general during our initial planning, we ended up using JUnit to test most of our user-defined functions, with the goal of having everything tested. This has greatly helped us in developing these functionalities in isolation and in splitting into separate tasks, allowing for more easy collaboration.



## Current Progress

> Your progress so far. Which tasks have you implemented?

In order to plan and coordinate the project, each task was divided into fine-grained subtasks represented with GitLab Issues.
As a first step for each task, a code skeleton consisting of the topology of the dataflow graph was built with stubs for the individual user-defined functions.
Afterwards the functions are implemented and each non-trivial user-defined function has either a unit test or an integration test depending on whether the functionality relies on state.

### Active Posts Statistic (#1)  
`estimated progress 95%`

Almost all issues for the active post statistics task are completed.
This includes the overall dataflow skeleton with the active post windowing logic, the comment to post id mapping, the comment/reply count and the unique interacting people count along with the respective tests.
Additionally, the outline for a full integration test has been created.

### Recommendations (#2)
`estimated progress 80%`

The functionality of the pipeline for the recommendation of new friends is largely implemented.
This includes mapping and aggregating events into user embeddings, select 10 users for recommendations, calculating similarity between user embeddings, filter existing friends and selecting top 5 most similar users all based on the activity of the last 4 hours and updated every hour.
Additionally, a full integration test has been implemented as a skeleton.


### Unusual Activity Detection  (#3)
`estimated progress 20%`

The skeleton for the unusual activity detection pipeline is implemented and relies as described in the design document on several independent features
which are then combined in an ensemble approach to decide if a user is suspicious.
The work on two features (timespan between user interactions and post/comment content) is already in progress.
Additionally a first integration test skeleton is ready for future development.


## Remaining Work

> Outstanding tasks and a timeline for completion.

The remaining issues were incorporated in a timeline for completion of the project.
For each task there is an additional time buffer in case the integration test of the complete pipeline results in new issues.

![timeline.png](timeline.png)

## Divergences from Original Plan

> Any divergence from your original plan. Did you change your mind about a tool you wanted to use or an algorithm?

TODO: sketch for kafka postid mapping?

Contrary to our initially ideated approach of not having intermediate results in external systems, our new solution outputs commentId - postId mappings into a Kafka topic. The pipelines can then consume this topic to enrich replies with the postId.
This choice increases latency in case a group of dependent comments arrive almost concurrently but brings the advantage of avoiding broadcasting and buffering each comment to every node in order to build up the complete comment tree. In a realistic scenario in which most dependent comments arrive with a certain delay which is greater than the latency of writing and reading from Kafka, there is no additional latency.
