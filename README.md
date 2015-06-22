# remap

Remap is an implementation of MapReduce in pure python.

The name is derived from the stages of reduce, map and python: REduce MApper Python
and the name itself is a pretty powerful reference that should be easy to remember.

The focus of this implementation is on:
- Very low complexity and number of steps for installation
- From the perspective of the app developer, no difference if run locally or in a thousands node cluster
- Sensible, minimalistic "expert" API, no specific API for I/O or k,v pair handling
- Support for multi-splitting of data (one mapper with multiple reducer 'sets')
- Uses existing "infrastructure" solutions to solve hard stuff (distributed fs, etc)
- Keeps input and output files clear of MR stuff, so the intermediate files also contain interpretable data
  to make debugging easier

See the wiki for more information:

https://github.com/gtoonstra/remap/wiki

