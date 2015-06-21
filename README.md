# mreasy

Mr. Easy is an implementation of MapReduce in pure python

The focus of this implementation is on:
- Very low complexity and number of steps for installation
- From the perspective of the app developer, no difference if run locally or in a thousands node cluster
- Sensible, minimalistic "expert" API, no specific API for I/O or k,v pair handling
- Support for multi-splitting of data (one mapper with multiple reducer 'sets')
- Uses existing "infrastructure" solutions to solve hard stuff (distributed fs, etc)
- Pristine input/output files. I.e., you can run this on and against anything
- Targeted for modest 3-10 node clusters


