(ns 
 crane.cluster
 (:use clojure.contrib.shell-out)
 (:use clojure.contrib.java-utils)
 (:use clj-ssh.ssh)
 (:use crane.ec2)
 (:use crane.config)
 (:use crane.remote-repl)
 (:import java.io.File)
 (:import java.util.ArrayList)
 (:import java.net.Socket))

(defn socket-repl
"Socket class, connected to node remote repl"
  [#^String node]
  (Socket. node 8080))

(defn push
  "takes a session, from, and to, channel-from-to triples.  sftp puts from->to."
  ([channel from to] (push [[channel from to]]))
  ([channel pushes]
      (doall (map (fn [[from to]]
		     (sftp channel :put from to))
		   pushes))))

(defn push-files [sessions pushes]
  (push sessions (map 
		  (fn [[x y]] [(file x) y])
		  pushes)))