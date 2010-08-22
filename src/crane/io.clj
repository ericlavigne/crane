(ns crane.io
  (:require [clojure.contrib.duck-streams :as ds]))

(defn read-from-classpath
  "Returns a String for the classpath resource at the given path."
  [path]
  (ds/slurp*
   (.getResourceAsStream
    (.getClassLoader
     (class *ns*)) path)))

(defn path-to-resource [resource]
  (.getPath
   (.getResource
    (.getClassLoader (class *ns*))
    resource)))
