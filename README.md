#PyCharmer

As OOZIE works for Hadoop, PyCharmer is a python workflow framework

##Overview

The main idea of this tool is to clarify the workflow and its input/output information while you *write or read* it, not execute it. We separate the framework into four major components: logic unit, plan flow, parameters and logging.

**Logic unit**, or job, let you define reusable and self-documented execution unit. The things you focus here is action on the given inputs. All the inputs should be ready-to-use before the job starts (e.g. no file path composing in the job).

**Plan flow** could be treat as a Directed Acyclical Graph (DAG). Most time, you can design it just in tree. However, you can also consider graph with certain constraints/conditions to create a plan with fault-tolerance (e.g. retrying to allocate a Hadoop job).

**Parameters** are the soul, or *context*, of the flow body. They represent in input and output of jobs (one output may be the input of following job). They can be got from files, codes or even runtime evaluations. Parameters should be defined with scopes (e.g. shared between group of jobs or shared in whole process).

**Logging** helps you to understand what happens based on the flow with give *context* (e.g. input). it's a built-in functionality for every logic unit. It provides hierarchical format for displaying the output of plan.


##Components

###JobNode

We can encapsulate business logic into `JobNode`. Say, checking some computation or executing a shell command. However, the logic should be reusable in different context. Therefore, we extract the logic implementation from job to  *callback* method, and treat job as task-descriptor in the whole plan flow.

To create a `JobNode`, you have to give an **id**, a **description** and a **callback** method. Besides, you need to describe the expected input/output for the job.

###JobBlock

Some job needs several sub-jobs to finish the work. We provide `JobBlock` to encapsulate small `JobNode`s. From a higher point of view, you could treat the `JobBlock` as a single `JobNode`. Yes, they implement the same interface, `Job`. For convenience, we referred them to `Job` in the following explanation.

###Job Plan

We treat one execution path between two jobs as one job plan; a graph of well-defined plans forms a plan flow. Each plan comprises of three parts: **starting job**, expected execution result of the starting job (we call it as **state**) and the **destination job**. To compose a complete plan flow, you should list all possible plans between jobs, including what's the destinations for error or warning states.

###Configuration

We provide a mechanism, called configuration, to manipulate the parameters in the flow. It collects parameters from files, codes and job outputs; keeping them in key-value pairs with corresponding applicable scopes (e.g. in the same job block or as global in the flow). You can access them by `self.input['key']` in the job callback.

Configuration also allows you to generate a parameter value from existing parameters. Imagine you have a series of jobs share same execution logic but should work in different directories. All the directory paths shares a parent directory; they only differ in deepest level. You can easily prepare the parameter value by using bracket-variable — `/some/parent/directory/with/different/[part]` and initialize different values for the parameter `part`.

###Logger

The `Job` class maintains a `Logger` object as class variable. This allows you to add a log without initiating any object but it may not be convenient for logging in job objects (to access a class variable, you need to type more). Thus, in any job objects, we made delegation so that you could use log method from `self`


##Build a flow

To build a flow, we apply top-down strategy.

First, create a `JobBlock` as a top-level wrapper. Then, define its inner job plans — all you need here is what jobs you want and how they move to each other; actual job implementations could be considered later.

```python
	wrapper = JobBlock(id='entry job', desc='...')
	wrapper.add_plan(from_job_id='job_a', state=Job.DONE, to_job_id='job_b')
```
Then, you have to describe the job.

```python
    j = JobNode(id='job_a',desc='...')
    j.need_input('some_input', 'foo')
    j.set_callback(some_callback)
    wrapper.add_sub_job(j)
```

Then, implement a compatible callback.
```python
    def some_callback(self):
        some_config = self.input['some_input']
        is_something_happened = None
        try:
            # do something and set is_something_happened ...
            if is_something_happened:
                return Job.SKIP
            else:
                return Job.DONE
        except:
            return Job.ERRO
```

Finally, execute your wrapper, the most outer Job.
```python
    wrapper.execute()
```