(ns crane.sql
  (:use clojure.contrib.sql))

(def prod-db {:classname "com.mysql.jdbc.Driver"
	      :subprotocol "mysql"
	      :subname "//localhost:3306/production"
	      :user "root"
	      :password "root"})

(defn create-blogs
  "Create a table to store blog entries"
  [db]
  (with-connection db
    (create-table
     :blogs
     [:id :integer "PRIMARY KEY" "AUTO_INCREMENT"]
     [:title "varchar(255)"]
     [:description :text]
     [:body :text]
     [:published "TIMESTAMP NULL DEFAULT NULL"]
     [:created "TIMESTAMP NULL DEFAULT NULL"]
     [:updated "TIMESTAMP NULL DEFAULT NULL"])))

(defn drop-blogs
  "Drop the blogs table"
  [db]
  (with-connection
    db
    (transaction
     (try
      (drop-table :blogs)
      (catch Exception _)))))

(defn blogs [db]
(with-connection db 
   (with-query-results rs ["select * from blogs"] 
     ; rs will be a sequence of maps, 
     ; one for each record in the result set. 
     (into [] rs))))

(defn insert-blog-entry
  "Insert data into the table"
  [db title body]
  (with-connection
    db
    (transaction
     (insert-values
      :blogs
      [:title :body]
      [title body]))))

(defn delete-blog-entry
  "Deletes a blog entry given the id"
  [db id]
  (with-connection db
    (delete-rows :blogs ["id=?" id])))

(defn create-landing-page
  [db]
  (with-connection db
    (create-table
     :landing
     [:id :integer "PRIMARY KEY" "AUTO_INCREMENT"]
     [:email "varchar(255)"]
     [:username "varchar(255)"])))

(defn insert-landing
  [db email username]
  (with-connection
    db
    (transaction
     (insert-values
      :landing
      [:email :username]
      [email username]))))

(defn users [db]
(with-connection db 
   (with-query-results rs ["select * from landing"] 
     ; rs will be a sequence of maps, 
     ; one for each record in the result set. 
     (into [] rs))))


 ;; (defroutes sql-app
 ;;  (GET "/hi" [] "<h1>Hello World Wide Web!</h1>")
 ;;  (GET "/json" [] (encode-to-str [{:foo "bar"} {:bar "baz"}]))
 ;;  (GET "/blogs" []
 ;;       (json-with-dates (blogs prod-db) :published)))
