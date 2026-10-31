**Assignment 4 — Distributed Stream Processing**  
**AITU Research Event Analytics with Kafka and Spark**

*Astana IT University \| Real-time Data Analysis \| 2026–2027*

All datasets and operational scenarios in this document are synthetic educational data created for assessment. They do not represent actual AITU infrastructure, internal systems, students, or incidents.

# **Scenario**

Design a synthetic distributed streaming pipeline for an AITU research analytics platform. Multiple producers generate research-job events; the platform consumes, aggregates, and monitors them in near real time.

## **Given stream schema**

| Field | Description |
| :---- | :---- |
| event\_id | Unique event identifier |
| timestamp | Event time |
| research\_group | Group identifier |
| job\_type | simulation / ml / image / text |
| duration\_sec | Job duration |
| status | completed / failed |
| cpu\_pct | Worker CPU utilization |

## **Initial event data**

| event\_id | timestamp | group | job\_type | duration\_sec | status | cpu\_pct |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| E001 | 14:00:01 | G01 | ml | 42 | completed | 61 |
| E002 | 14:00:02 | G02 | image | 75 | completed | 74 |
| E003 | 14:00:03 | G01 | text | 18 | completed | 38 |
| E004 | 14:00:04 | G03 | simulation | 95 | failed | 92 |
| E005 | 14:00:05 | G02 | ml | 51 | completed | 68 |
| E006 | 14:00:06 | G01 | image | 88 | completed | 81 |
| E007 | 14:00:07 | G03 | ml | 64 | failed | 89 |
| E008 | 14:00:08 | G02 | text | 22 | completed | 44 |
| E009 | 14:00:09 | G01 | simulation | 120 | completed | 94 |
| E010 | 14:00:10 | G03 | image | 70 | completed | 78 |

# **Part A — Streaming Architecture (6 points)**

Draw a Kafka-based architecture with at least: producers, topic, partitions, consumer group, Python processor, aggregation layer, and monitoring.

| Component | Purpose | Failure/scale consideration |
| :---- | :---- | :---- |
| Producer |  |  |
| Kafka topic |  |  |
| Partition |  |  |
| Consumer group |  |  |
| Processor |  |  |
| Monitoring |  |  |

# **Part B — Kafka Producer/Consumer (7 points)**

1. Create a Python producer that sends the supplied events.  
2. Create a consumer that reads events and validates the schema.  
3. Count completed and failed jobs.  
4. Calculate average duration by job\_type.  
5. Calculate average CPU utilization by research group.

| Job type | Completed | Failed | Average duration |
| :---- | :---- | :---- | :---- |
| ml |  |  |  |
| image |  |  |  |
| text |  |  |  |
| simulation |  |  |  |

# **Part C — Windowed Stream Processing (7 points)**

Implement a 5-second event-time window. For each window calculate event count, failure rate, and average CPU.

| Window | Events | Count | Failure rate | Average CPU |
| :---- | :---- | :---- | :---- | :---- |
| 14:00:00–14:00:05 |  |  |  |  |
| 14:00:05–14:00:10 |  |  |  |  |
| 14:00:10–14:00:15 |  |  |  |  |

# **Part D — Spark Structured Streaming (6 points)**

Implement an equivalent streaming transformation using Spark Structured Streaming. The output must contain window, job\_type, count, and average duration.

| Output field | Type | Transformation |
| :---- | :---- | :---- |
| window |  |  |
| job\_type |  |  |
| count |  |  |
| avg\_duration |  |  |

# **Part E — Throughput and Latency (4 points)**

Measure the processing performance using a synthetic load. Report at least three load levels.

| Input rate (events/s) | Observed throughput | Average latency (ms) | CPU % | Bottleneck |
| :---- | :---- | :---- | :---- | :---- |
| 10 |  |  |  |  |
| 100 |  |  |  |  |
| 500 |  |  |  |  |

Explain whether the system scales sufficiently and identify one optimization.

# **Submission**

* Architecture diagram  
* Python Kafka producer/consumer  
* Spark Structured Streaming code  
* Windowed analysis  
* Performance table  
* Technical report and oral defense

**STUDENT TECHNICAL REPORT / SUBMISSION RECORD**

**Assignment 4 — Distributed Stream Processing**  
**AITU Research Event Analytics with Kafka and Spark**

This section is a formal submission and acceptance record. Complete all applicable fields and attach evidence from your own implementation. The common synthetic scenario and dataset do not make student submissions interchangeable.

# **1\. Student and Submission Information**

| Required field | Student entry |
| :---- | :---- |
| Student full name | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Student ID | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Group / cohort | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Instructor | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Submission date | \_\_\_\_ / \_\_\_\_ / \_\_\_\_\_\_ |
| Individual variant / task code | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Git repository / project link | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Python / tool versions | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |

# **2\. Work Record**

| Item | Student entry |
| :---- | :---- |
| Work start date | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Work completion date | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Main libraries / technologies | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Source files / notebook | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Test / experiment files | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Final repository commit/version | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |

# **3\. Report Structure to Complete**

For each section, provide a concise technical explanation and cite your own code, calculation, experiment, plot, log, screenshot, configuration, or other artifact. Generic copied explanations without individual evidence are insufficient.

## **4.1 Streaming Architecture**

Provide and explain your Kafka architecture with producers, topic, partitions, consumer group, Python processor, aggregation layer, and monitoring. Discuss failure and scaling considerations.

| Required evidence | Student response / reference |
| :---- | :---- |
| Technical explanation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Code / file / function / configuration reference | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Result / calculation / experiment evidence | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Student-specific decision or observation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |

## **4.2 Kafka Producer and Consumer**

Document producer/consumer implementation, schema validation, completed/failed job counts, average duration by job type, and average CPU by research group. Reference the exact code and output.

| Required evidence | Student response / reference |
| :---- | :---- |
| Technical explanation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Code / file / function / configuration reference | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Result / calculation / experiment evidence | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Student-specific decision or observation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |

## **4.3 Windowed Stream Processing**

Complete the 5-second event-time windows and calculate event count, failure rate, and average CPU. Explain window boundaries and processing logic.

| Required evidence | Student response / reference |
| :---- | :---- |
| Technical explanation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Code / file / function / configuration reference | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Result / calculation / experiment evidence | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Student-specific decision or observation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |

## **4.4 Spark Structured Streaming**

Document the equivalent Spark transformation and the output fields window, job\_type, count, and avg\_duration. Reference the exact code/configuration used.

| Required evidence | Student response / reference |
| :---- | :---- |
| Technical explanation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Code / file / function / configuration reference | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Result / calculation / experiment evidence | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Student-specific decision or observation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |

## **4.5 Throughput and Latency**

Report measurements at 10, 100, and 500 events/s, including observed throughput, average latency, CPU, and bottleneck. Explain scalability and one optimization.

| Required evidence | Student response / reference |
| :---- | :---- |
| Technical explanation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Code / file / function / configuration reference | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Result / calculation / experiment evidence | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Student-specific decision or observation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |

## **4.6 Engineering Interpretation**

State the main architectural bottleneck, explain behavior under increasing event volume, and justify the optimization using measured evidence.

| Required evidence | Student response / reference |
| :---- | :---- |
| Technical explanation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Code / file / function / configuration reference | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Result / calculation / experiment evidence | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Student-specific decision or observation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |

# **4\. Individual Work and Anti-Plagiarism Record**

Students may work from the same assignment statement and synthetic data. However, each submission must contain independently produced implementation evidence. Copying another student's source code, tables, calculations, plots, screenshots, logs, API responses, architecture, wording, or conclusions is not acceptable.

* Use the assigned individual variant/task code in the report, README, and repository.  
* Provide the exact Kafka topic/partition configuration and source filenames used.  
* Include execution-generated consumer output and window results.  
* Include measured performance evidence, not estimated values.  
* Document one scaling/design decision made by you and why.  
* Record one implementation or configuration problem you personally solved.  
* Be prepared to explain or modify one Kafka/Spark component during oral defense.

| Individual evidence | Student entry |
| :---- | :---- |
| My unique technical/design decision | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| My own test/experiment | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Problem/bug I personally encountered and solved | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Exact code/repository/commit reference | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| One limitation of my implementation | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| One improvement I propose | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |

# **5\. Evidence and Submission Checklist**

* ☐ Technical report completed  
* ☐ Source code / notebook submitted  
* ☐ Required tables and calculations completed  
* ☐ Required diagrams / plots / dashboard evidence included  
* ☐ Execution logs or reproducible experiment evidence included  
* ☐ README and run instructions included where required  
* ☐ Data/schema/configuration files included where required  
* ☐ Performance / analytical results included where required  
* ☐ Oral defense / demonstration completed

# **6\. Student Declaration of Originality**

I confirm that this submission represents my own work. I have not copied another student's source code, calculations, tables, architecture, plots, screenshots, logs, results, or written explanations. Any external material or reused code has been identified. I understand that the instructor may request a live demonstration, code modification, reproduction of calculations, or explanation of any submitted component.

| Role | Name | Signature | Date |
| :---- | :---- | :---- | :---- |
| Student / Executor | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ | \_\_\_\_\_\_\_\_\_\_\_\_ |
| Instructor / Accepted by | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ | \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ | \_\_\_\_\_\_\_\_\_\_\_\_ |

# **7\. Instructor Acceptance Record**

| Verification item | Status / comment |
| :---- | :---- |
| Report structure completed | ☐ Yes  ☐ No   \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Required artifacts submitted | ☐ Yes  ☐ No   \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Evidence is reproducible/sufficient | ☐ Yes  ☐ No   \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Individual contribution is identifiable | ☐ Yes  ☐ No   \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Oral defense / demonstration completed | ☐ Yes  ☐ No   \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |
| Final status | ☐ Accepted  ☐ Revision required |

