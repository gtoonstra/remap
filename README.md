# remap

Remap is an distribution execution engine (for lack of a better description) written in 100% pure python. You can kick off distributed processes like map/reduce from a remap monitor. The platform figures out which nodes and cores are available to run the jobs on and it will distribute the work across those nodes and track its progress.

At the moment map/reduce is implemented and working; that is... the monitor, management and execution bit. You need to use a REST client to kick off the job from the monitor with a bit of simple json. Work is in progress to make that process a bit easier to understand.

Remap is very new and targeted at small-scale installations. The whole idea of remap is for developers, researchers and tinkerers to be able to put an environment together very quickly, so you can get to your algorithm implementations as quickly as possible.

So the focus of this implementation is on:
- Very low complexity and number of steps for installation
- Can run locally on a single machine or on a couple of machines without much effort
- Sensible, minimalistic API that doesn't get in your way (you have full control over the data flow in code)
- Relies on mature projects to solve 'the difficult stuff' (distributed filesystems, high performance messaging, etc)
- Input and output files are always in an interpretable format, but you decide that.

If this sounds interesting, please follow the steps on the wiki to take it for a quick test run:

https://github.com/gtoonstra/remap/wiki

##Help wanted

I'm looking for people who're interested in helping out. There's work to be done on design, plumbing, reliability testing, UI for a dashboard and a small nodejs server to interact with the cluster. See the wiki for details.
