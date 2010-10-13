(ns 
 crane.core
 (:use clojure.contrib.java-utils)
 (:use clj-ssh.ssh)
 (:use crane.ec2)
 (:use crane.config)
 (:use crane.remote-repl)
 (:use [clojure.java.shell :only [sh]])
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

(defn rsync
  ([priv-key-path host user [from to]]
     (let [e-arg (str "ssh -i " priv-key-path)]
       (sh "rsync" "-avz" "-e" e-arg from (str user "@" host ":" to))))
  ([host user [from to]]
     (sh "rsync" "-avz" from (str user "@" host ":" to))))

(defn bootstrap
  "bootstraps a machine.
  takes a config and creds map, and...
  -installs packages from apt
  -pushes artifacts to the machine
  -runs programs"
  ([k] (apply bootstrap (conf-cred k)))
  ([conf cred]
     (if (or (keyword? conf)
             (keyword? cred))
       (apply bootstrap (conf-cred conf cred))
       (do 
         (println (:private-key-path cred) (:user cred) (:host conf))
         (with-ssh-agent []
           (add-identity (:private-key-path cred))
           (let [session (session (:host conf)
                                  :username (:user cred)
                                  :strict-host-key-checking :no)]
             (with-connection session
               (do
                 (dorun (map #(ssh session :in %)
                             (:install conf)))
                 (dorun (map #(rsync (:private-key-path cred)
                                     (:host conf)
                                     (:user cred) %)
                             (:push conf)))
                 (dorun (map #(ssh session :in %)
                             (:run conf)))))))))))

(defn bootstrap-cluster
  "Bootstrap a cluster of machines from a sequence of configurations."
  [confs]
  (dorun (pmap #(bootstrap % (creds %))
               confs)))

;;TODO: hack to run target fns in deploy.clj on server machines since we use lein for client.  figure out a better option.  currently required to go at bottom of deploy.clj
(defmacro end-targets
  []
  `(doall (map
           #((ns-resolve *ns* (symbol %)))
           *command-line-args*)))