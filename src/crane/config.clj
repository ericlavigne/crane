(ns crane.config
  (:require [clojure.contrib.duck-streams :as ds])
  (:use clojure.contrib.java-utils)
  (:import [com.xerox.amazonws.ec2 Jec2 
	    InstanceType LaunchConfiguration
	    ImageType EC2Exception
	    ReservationDescription
	    ReservedInstances]))

(defn this-path [] (.getCanonicalPath (java.io.File. ".")))
(defn path-exists? [path] (.exists (java.io.File. path)))

(def instance-types
 {:m1.small InstanceType/DEFAULT
  :m1.large InstanceType/LARGE           
  :m1.xlarge InstanceType/MEDIUM_HCPU
  :c1.medium InstanceType/XLARGE
  :c1.xlarge InstanceType/XLARGE_HCPU})

;;TODO: thjis fn belogs with the string-foo
(defn replace-all
"replaces all of the left hand side tuple members with right hand side tuple members in the provided string."
[s pairs]
(if (= 0 (count pairs)) s
 (let [[old new] (first pairs)]
   (replace-all (.replaceAll s old new) (rest pairs)))))

(defn user-data 
"read user data file from the config-path given and replace the aws key and secrey key with those from the creds-path given."
([config-path creds]
  (let [data (ds/slurp* config-path)]
    (replace-all data
      [["AWS_ACCESS_KEY_ID" (:key creds)]
       ["AWS_SECRET_ACCESS_KEY" (:secretkey creds)]])))

([config-path creds master-host]
  (replace-all (user-data config-path creds)
      [["MASTER_HOST" master-host]])))

(defn get-bytes [x] (.getBytes x))

(defn to-ec2-conf [config]
    (merge config
	   {:instance-type ((:instance-type config) instance-types)}))

(defn conf
  "provide the path to your config info directory containing aws.clj
for now just ami:
 
 {:ami \"ami-xxxxxx\"}
 
useage: (read-string (slurp* (conf \"/foo/bar/creds/\"))))
 
"
  [path]
  (let [config (read-string 
		(ds/slurp* (ds/file-str path "aws.clj")))]
    (to-ec2-conf config)))

(defn init-remote
"
provide the path to your hadoop config directory containing 
hadoop-ec2-init-remote.sh
"
[path] (ds/file-str path "hadoop-ec2-init-remote.sh"))

(defn mk-path [& args]
  (apply str (conj (into [] (map #(if (.endsWith % "/") % (str % "/"))
		  (butlast args))) (last args))))

(defn creds-path [conf]
  (let [c (:local-creds conf)]
    (if (path-exists? c)
      c
      (:server-creds conf))))

(defn creds 
"provide the path to your creds home directory containing creds.clj
key secrey-key pair stored in map as:

 {:key \"AWSKEY\"
  :secretkey \"AWSSECRETKEY\"}
  :keyname \"aws rsa key file name\"}

useage: (read-string (slurp* (creds \"/foo/bar/creds/\"))))

"
[cred]
(let [path (if (map? cred) (creds-path cred)
	       cred)]
  (read-string (ds/slurp* (mk-path path "creds.clj")))))

;;TODO: belongs in a seperate project for transformations.  duplicated from infer.features
(defn flatten-seqs
  "Takes any nested combination of sequential things (lists, vectors,
  etc.) and returns the lowest level sequential items as a sequence of sequences."
  [x]
  (let [contains-seq? (partial some sequential?)]
    (filter (complement contains-seq?)
	    (rest (tree-seq contains-seq? seq x)))))

(defn replace-keys
"replace any keywords in s with the v at the k in m."
[m s]
(into [] (map #(if (keyword? %) (% m) %) s)))

(defn wildcard-dir [d]
  (if (.isDirectory (java.io.File. d))
	  (str d "*")
	  d))

(defn wildcards [pushes]
  (map (fn [[from to]]
	 [(wildcard-dir from) to])
       pushes))

(def default-pushes
     [[[[:local-root "/src"]
	[:local-root "/lib"]
	[:local-root "/crane"]] :server-root]
      [:local-creds :server-creds]])

;;TODOD: flatten all but last.
;;extract and test these fns.
(defn expand-pushes [c]
  (let [pushes (concat default-pushes (:push c))
	new-pushes
	 (flatten-seqs
	  (map (fn [p]
		 (if (not (coll? (first p)))
		   (replace-keys c p)
		   (let [froms (first p)
			 [to] (replace-keys c [(second p)])
			 ps (map (fn [from to]
				   (let [fr (if (string? from) from
						(apply str (replace-keys c from)))]
				   [fr to]))
				 froms
				 (repeat to))]
		     ps)))
	       pushes))
	new-conf (assoc c :push new-pushes)]
    new-conf))

(defn target-calls [c ts]
  (apply str
   (replace-keys c
		 (flatten
		  ["java -cp " :server-root "src/:" :server-root "lib/* clojure.main "
		   :server-root "crane/deploy.clj " (interleave ts (repeat " "))]))))

(defn expand-cmds
  [conf*]
  (let [cs [:install :run]
	rep (fn [conf c]
	      (let [cmds (c conf)
		    new-cmds (map (fn [co]
				    (cond (string? co) co
					  (= :targets (first co))
					  (target-calls conf (rest co))
					  :else (apply str
						       (replace-keys conf co))))
				  cmds)
		    new-conf (assoc conf c new-cmds)]
		new-conf))
	fix-all (fn [ks cnf]
		  (if (empty? ks) cnf
		      (recur (rest ks) (rep cnf (first ks)))))]

    (fix-all cs conf*)))

(defn expand-local-creds [conf*]
  (let [expanded (apply str (replace-keys conf* (:local-creds conf*)))]
    (assoc conf* :local-creds expanded)))

(defn read-conf [l s]
 (merge {:local-root (this-path)}
    (read-string
     (slurp
      (if (path-exists? l)
	l s)))))

(defn rooted [path] (str (this-path) "/" path))

(defn expand-conf [local server]
   ((comp expand-cmds expand-pushes expand-local-creds read-conf)
    (rooted local) server))