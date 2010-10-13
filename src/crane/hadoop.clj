(ns
 #^{:doc
    "required kv pairs in config map for hadoop clusters.
:image         ;;your aws ec2 image 
:instance-type ;;desired instance type
:group         ;;cluster group; will be the group with slaves
:instances     ;;number of slaves, does not include jobtracker, namenode
:creds         ;;path to dir with file containing  aws creds;
                 creds.clj should be a map, with vals being aws creds
:jar-files     ;;jar files to push to master, must have socket repl jar
                 and project jar w/ dependencies
                 [[from to] [from to]]
:hadooppath    ;;path to hadoop dir on remote machine
:hadoopuser    ;;hadoop user on remote machine
:mapredsite    ;;path to mapred-site.xml template
:coresite      ;;path to core-site.xml template
:hdfssite      ;;path to hdfs-site.xml template

Optional keys in config {}

:zone          ;;ec2 cluster location
:push          ;;vector of strings, [[from to] [from to]]"}
crane.hadoop
 (:require [clojure.zip :as zip])
 (:require [clojure.contrib.duck-streams :as ds])
 (:use crane.core)
 (:use clojure.contrib.java-utils)
 (:use clj-ssh.ssh)
 (:use clojure.xml)
 (:use clojure.contrib.str-utils)
 (:use crane.ec2))

;; got xml renderer from http://www.erik-rasmussen.com/blog/2009/09/08/xml-renderer-in-clojure/
;; took out html stuff
(defn render-xml-attributes [attributes]
 (when attributes
   (apply str
     (for [[key value] attributes]
       (str \space (name key) "=\"" value \")))))
 
(defn render-xml [node]
   (if (string? node)
     (.trim node)
     (let [tag (:tag node)
           children (:content node)
           has-children? (not-empty children)
           open-tag (str \< (name tag)
                       (render-xml-attributes (:attrs node))
                       (if has-children? \> "/>"))
           close-tag (when has-children? (str "</" (name tag) \>))]
       (str
         open-tag
         (apply str (when has-children?
                      (for [child children]
                        (render-xml child))))
         close-tag))))

(defn parse-str-xml [s]
  (parse (new org.xml.sax.InputSource
              (new java.io.StringReader s))))

(defn make-classpath [config]
  (apply str (:hadooppath config) "/conf:"
             (interpose ":" (map
                             (fn [[x y]] y)
                             (:jar-files config)))))

(defn cluster-jt-name [cluster-name]
  (str cluster-name "-jobtracker"))

(defn cluster-nn-name [cluster-name]
  (str cluster-name "-namenode"))

(defn find-master
"find master finds the master for a given cluster-name.
if the cluster is named foo, then the master is named foo-jobtracker.
be advised that find master returns nil if the master has been reserved but is not in running state yet."
  [ec2 cluster-name]
  (first
   (running-instances ec2 (cluster-jt-name cluster-name))))

(defn find-namenode [ec2 cluster-name]
  (first
   (running-instances ec2 (cluster-nn-name cluster-name))))

(defn master-already-running? [ec2 cluster]
 (if (find-master ec2 cluster)
   true
   false))

(defn cluster-running? [ec2 cluster n]
  (and (master-already-running? ec2 cluster)
       (already-running? ec2 cluster n)))

(defn cluster-instance-ids [ec2 cluster]
  (instance-ids
   (concat
    (find-reservations ec2 cluster)
    (find-reservations ec2 (cluster-jt-name cluster))
    (find-reservations ec2 (cluster-nn-name cluster)))))

(defn stop-cluster
  "terminates the master and all slaves."
  [ec2 cluster]
  (terminate-instances ec2 (cluster-instance-ids ec2 cluster)))

(defn job-tracker-url [{host :public}]
  (str "http://" host ":50030"))

(defn name-node-url [{host :public}]
  (str "http://" host ":50070"))

(defn namenode [ec cluster-name]
  (name-node-url
   (attributes
    (find-namenode ec cluster-name))))

(defn tracker
"gets the url for job tracker."
  [ec cluster-name]
  (job-tracker-url
    (attributes
      (find-master ec cluster-name))))

(defn create-mapred-site [template-file tracker-hostport]
  (let [template-xml (zip/xml-zip (parse template-file))
        insertxml (parse-str-xml (str "<property><name>mapred.job.tracker</name><value>" tracker-hostport "</value></property>"))
        ]
        (render-xml (-> template-xml (zip/insert-child insertxml) zip/root))))

(defn create-core-site [template-file namenode-hostport]
  (let [template-xml (zip/xml-zip (parse template-file))
        insertxml (parse-str-xml (str "<property><name>fs.default.name</name><value>" "hdfs://" namenode-hostport "</value></property>"))
        ]
    (render-xml (-> template-xml (zip/insert-child insertxml) zip/root))))

(defn launch-jobtracker-machine [ec2 config]
  (let
    [cluster-name (:group config)
     master-conf (merge config {:group (cluster-jt-name cluster-name)})]
    (ec2-instance ec2 master-conf)))

(defn launch-namenode-machine [ec2 config]
  (let
    [cluster-name (:group config)
    master-conf (merge config {:group (cluster-nn-name cluster-name)})]
    (ec2-instance ec2 master-conf)))

(defn launch-slave-machines
"launch n hadoop slaves as specified by :instances in conf"
  [ec2 conf]
  (ec2-instances ec2 conf))

(defn launch-hadoop-machines [ec2 config]
  (doall (pmap
    #(% ec2 config)
    [launch-jobtracker-machine
     launch-namenode-machine
     launch-slave-machines]))
  (Thread/sleep 1000))

(defn get-slaves-str [slaves]
  (let
    [slave-ips (map #(:private (attributes %)) slaves)]
    (apply str (interleave slave-ips (repeat "\n")))))

(defn hadoop-conf
"creates configuration, and remote shell-cmd map."
  [config]
  {:slaves-file (str (:hadooppath config) "/conf/slaves")
   :coresite-file (str (:hadooppath config) "/conf/core-site.xml")
   :hdfssite-file (str (:hadooppath config) "/conf/hdfs-site.xml")
   :mapredsite-file (str (:hadooppath config) "/conf/mapred-site.xml")
   :namenode-cmd (str "cd " (:hadooppath config) " && bin/hadoop namenode -format && bin/start-dfs.sh")
   :jobtracker-cmd (str "cd " (:hadooppath config) " && bin/hadoop-daemon.sh start jobtracker")
   :tasktracker-cmd (str "cd " (:hadooppath config) " && bin/hadoop-daemon.sh start tasktracker")
   :hdfs-site (slurp (:hdfssite config))})

(defn push-mapred
  "push mapreduce site.  should be pushed to master, name, and slaves."
  [ec2 config]
  (let [mapred-file (create-mapred-site (:mapredsite config)
                                        (str (.getPrivateDnsName
                                              (find-master
					       ec2 (:group config)))
					     ":9000"))]
    #(push % mapred-file (:mapredsite-file (hadoop-conf config)))))

(defn push-coresite
  "push core site.  should be pushed to master, name, and slaves."
  [ec2 config]
  (let [coresite-file (create-core-site (:coresite config)
                                        (str (.getPrivateDnsName
                                              (find-namenode
					       ec2 (:group config)))))]
    #(push % coresite-file (:coresite-file (hadoop-conf config)))))

(defn push-hdfs
  "push hdfs site. should be pushed to master, name, and slaves."
  [ec2 config]
  (let [hdfs-file (:hdfs-site (hadoop-conf config))]
    #(push % hdfs-file (:hdfssite-file (hadoop-conf config)))))

(defn push-slaves
  "push slaves file to master and name node."
  [ec2 config]
  (let [slaves-file (get-slaves-str
                     (running-instances ec2 (:group config)))]
    #(push % slaves-file (:slaves-file (hadoop-conf config)))))

(defn push-master-files
  "push files to master node."
  [ec2 config]
  (let [files (map
	       (fn [[x y]] [(file x) y])
	       (concat (:jar-files config) 
		       (:push config)))]
    #(map
      (fn [[x y]] (push % x y))
      files)))

(defn launch-cluster [ec2 config]
  (let [_ (launch-hadoop-machines ec2 config)
	master (find-master ec2 (:group config))
	name-node (find-namenode ec2 (:group config))
	slaves (running-instances ec2 (:group config))
	_ (in-ec2-session master config 
			  (fn [s]
			    (map #(% s)
				 [push-mapred 
				  push-coresite 
				  push-hdfs 
				  push-slaves 
				  push-master-files])))
	_ (in-ec2-session name-node config 
			  (fn [s]
			    (map #(% s)
				 [push-mapred 
				  push-coresite 
				  push-hdfs 
				  push-slaves])))
	_ (dorun (pmap #(in-ec2-session % config 
					(fn [s]
					  (map (fn [f] (% s))
					       [push-mapred 
						push-coresite 
						push-hdfs])))
		       slaves))
	cmds (hadoop-conf config)
	_ (in-ec2-session name-node config
			  (fn [s]
			    (ssh s :in (:namenode-cmd cmds))))
	_ (in-ec2-session master config
			  (fn [s]
			    (ssh s :in (:jobtracker-cmd cmds))))
	_ 
	(dorun (pmap
		#(in-ec2-session % config
				 (fn [s] (ssh s :in (:tasktracker-cmd cmds))))
		slaves))]))

(defn master-ips
"returns list of namenode and jobtracker external ip addresses"
  [ec2 config]
  (list (:public (attributes (find-master  ec2 (:group config))))
        (:public (attributes (find-namenode  ec2 (:group config))))))

(defn nn-private
"returns namenode private address as string"
  [ec2 config]
  (:private (attributes (find-namenode ec2 (:group config)))))

(defn jt-private
"returns jobtracker private address as string"
  [ec2 config]
  (str (:private (attributes (find-master ec2 (:group config)))) ":9000"))

(defn next-split [s] (.indexOf s "\t"))
(defn first-word [s]
  (let [i (next-split s)]
    (if (= i -1) s
	(.substring s 0 i))))

;;use java stream tokenizer or something?
(defn rest-words [s]
  (let [i (next-split s)]
    (if (= i -1) nil
	(.substring s (+ i 1)))))

(def partfile-reader (comp read-string rest-words))
"
 (s3->clj s3 root-bucket rest partfile-reader))
"

(defn multiline-partfile-reader [f]
  (map 
   partfile-reader 
   (line-seq (ds/reader f))))

(defn from-lines
"
 (s3->clj s3 root-bucket rest from-lines))
"
[#^String s]
  (map 
   read-string
   (.split s "\n")))