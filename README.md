# remap

Remap is capable of running many cores in a distributed fashion over the network and orchestrate work. It's impossible to make a 100% generic daemon that does this, so there are modules involved that direct the process flow and status tracking of the workers on the network, which eventually determines how the algorithm behaves.

Remap is mostly written in pure python with the exception of a specialized broker for "vertex" tasks. The platform figures out if and how many nodes and cores are available to run your algorithm. The way this works is that you have a python script file with your map/reduce or vertex functions, which must be available to all nodes (either locally copied or from a network drive). Then you just start the job from the monitor and watch the fireworks.

Remap uses an HTTP Rest interface. See the "http_interface" file (which should be really easy to read) for the endpoints. Remap is integrated with airflow too (see airbnb), so you can have pretty advanced workflows where map/reduce or vertex jobs get involved.

Remap is too new for serious production work, so it's targeted at small-scale installations. The whole idea of remap is for developers, researchers and tinkerers to be able to put an environment together very quickly, so you can work on your algorithm implementations as quickly as possible.

Other issues I think are important:
- Minimal code quantity and steps for installation
- Run locally on a single machine or on a couple of machines without much effort
- Sensible, minimalistic API that doesn't get in your way (you have full control over the data flow in code, maybe even too much)
- Rely on mature projects to solve 'difficult stuff' (distributed filesystems, high performance messaging, etc)
- Input and output files can remain in an interpretable format, but you eventually decide that.

If this sounds interesting, please follow the steps on the wiki to take it for a quick test run:

https://github.com/gtoonstra/remap/wiki

##Help wanted

I'm looking for people who're interested in helping out. There's work to be done on design, plumbing, reliability testing, UI for a dashboard and a small nodejs server to interact with the cluster. See the wiki for details.

##Contact

There's a project mailing list for users and developers:

http://www.freelists.org/list/remap

You can also mail me at my gmail account. Use my github username.
