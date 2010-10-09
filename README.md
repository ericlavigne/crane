# crane
## Production deployment in clojure

Crane is build for production deployment of modern cloud services, web services, and distributed systems.

Crane comes from real world production and has evolved to support anything from a simple webserver, to a cluster of workers, a hadoop cluster, a sql database or a distributed database.

Crane works with AWS out of the box - it wraps AWS libs typica and jets3t for compute via ec2, blob storage via s3, queue service via sqs.

## How to deploy a web service.

crane has a leiningen plugin, so we'll add that with the crane dependency in our project.clj

      [crane "1.0-SNAPSHOT"]
      [crane/lein-crane "0.0.1"]

Add a deploy.clj file at project's root (right next to your project.clj)  You can call no-arg functions in you deploy.clj as targets using crane.

lein crane web

web must be a target in our deploy.clj, let's see:

    (defn web []
      (let [conf (config :webconfig)]
          (bootstrap conf conf)))

Here, we are deploying a webserver using crane's bootstrap capabilities.  Config flies are in the same dir as deploy.clj, and you get one by providing the name as a keyword.  Let's look at the config file, and walk through what bootstrap does.

      {:group "web"
       :web-root "/root/learner/"
       :server-creds "/root/learner/aws/"
       :local-creds [:local-root "/../Dropbox/aws/"]
       :user "root"
       :host "555.555.55.55"
       :private-key-path "/path/to/my/id_rsa"
       :push [[[[:local-root "/src"]
       	        [:local-root "/lib"]
		[:local-root "/deploy.clj"]
 		[:local-root "/webconfig.clj"]] :web-root]
		[:local-creds "/root/learner/aws"]]
       :run ["killall -9 java"
             ["java -cp " :web-root "src/:" :web-root "lib/* clojure.main "
 	     :web-root "deploy.clj server-learner"]]}

The call to config creates a configuration from the config file corresponding to the keyword argument.  This configuration is used to bootstrap the machine(s) indicated in the config.

[TODO: explain config vs. creds when I finish making the web looks like the aws.]

We have three phases in crane as of now; install, push, and run.

Aside from supplying basic configuration attributes like groups, project roots, and so forth, we are mosly building up vectors of strings of commands to run, or files to rsync.  Notive that there is some special shorthand for self-refnerence within config maps.  Anywhere that you could supply a string, you can supply a vector of strings and keywords, and the keyworkds will be replaced with their corresponding values from the config map.

**The install phase installs packages on your linux distro.**  You supply a vector of  install commands as strings.

**The push phase rsyncs local and remote files.**  Often you specify a number of files to sync from local locations to a single remote root.  Crane offers shorthand for this by providing a tupel where the first element is a vector of "froms", and the second element is the root, the "to."

Finally, **the run phase runs some commands** - this is where you start a webserver, worker processes, databases, or whatever this service is for.

[TODO: explain shorthand for running that calls a target in deploy on the server when we come up with it.]

[TODO: give example for work that uses the install phase.]
 

## How to start a hadoop ec2 cluster using crane.

Workflow:

Configuration maps:

creds.clj

{:key "AWS-KEY"
 :secretkey "AWS-SECRET-KEY"
 :private-key-path "/path/to/private-key"
 :key-name "key-name"}

conf.clj

{:image "ami-"
 :instance-type :m1.large
 :group "ec2-group"
 :instances int
 :instances int
 :creds "/path/to/creds.clj-dir"
 :push ["/source/path" "/dest/path/file"
        "/another/source/path" "/another/dest/path/file"]
 :hadooppath "/path/to/hadoop"          ;;used for remote pathing
 :hadoopuser "hadoop-user"              ;;remote hadoop user
 :mapredsite "/path/to/local/mapred"    ;;used as local templates
 :coresite "/path/to/local/core-site"
 :hdfssite "/path/to/local/hdfssite"}

Example:

;;read in config "aws.clj"
crane.cluster> (def my-conf (conf "/path/to/conf.clj-dir"))

;;create new Jec2 class
crane.cluster> (def ec (ec2 (creds "/path/to/creds.clj-dir")))

;;start cluster 
(launch-hadoop-cluster ec my-conf)

To build:

1. Download and install leiningen http://github.com/technomancy/leiningen
2. $ lein deps
4. $ lein install

## crane is part of clj-sys http://github.com/clj-sys

- Conciseness, but not at the expense of expressiveness, explicitness, abstraction, and composability.

- Simple and explict functional sytle over macros and elaborate DSLs.

- Functional parameterization over vars and binding.

- Libraries over frameworks.

- Build in layers.

- Write tests.

- Copyright (c) Bradford Cross and Jared Strate released under the MIT License (http://www.opensource.org/licenses/mit-license.php).