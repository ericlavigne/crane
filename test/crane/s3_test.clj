(ns crane.s3-test
  (:use clojure.test
	crane.s3))

(deftest append-clj-data
  (is (= [1 2]
	 (append [[1] [2]])))
  (is (= {:a 1 :b 2}
	 (append [{:a 1} {:b 2}])))
  (is (= '(1 2)
	 (append ['(1) '(2)])))
  (is (= "foo.bar"
	 (append ["foo." "bar"]))))