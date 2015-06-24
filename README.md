# remap

Remap is an implementation of MapReduce in pure python.

The name is derived from the stages of reduce, map and python: REduce MApper Python

and its specific order is a reference to rearranging data, which is what map-reduce does.

The focus of this implementation is on:
- Very low complexity and number of steps for installation
- Can run locally on a single machine or on a couple of machines without much effort
- Sensible, minimalistic API that doesn't get in your way (you have full control over the data flow)
- Support for multi-splitting of data (one single mapper outputting to multiple 'reducer sets'). So you can split data one to many very easily.
- Relies on other mature projects to solve 'the difficult stuff' (distributed filesystems, high performance messaging, etc)
- Input and output files are always in an interpretable format outside the MR framework.

If this sounds interesting, please follow the steps on the wiki to take it for a quick test run:

https://github.com/gtoonstra/remap/wiki
