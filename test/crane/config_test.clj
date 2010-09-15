(ns crane.config-test
  (:use clojure.test
        crane.config))

(deftest test-replace-all
  (is (= "foo baz"
	       (replace-all "foo bar" [["bar" "baz"]])))
  (is (= "foo baz bag"
	       (replace-all "foo bar biz" [["bar" "baz"] ["biz" "bag"]]))))

(deftest replace-keys-test
  (is (= ["foo" "bar" "baz"]
	 (replace-keys {:foo "foo"}
		       [:foo "bar" "baz"]))))

(deftest expand-pushes-test
  (is (= {:push [["l" "r"] ["l" "r"]]}
	 (expand-pushes {:push [[["l" "l"] "r"]]}))))

(deftest expand-cmd-test
  (is (= {:c "ls " :cmds ["ls foo"]}
	 (expand-cmds {:c "ls " :cmds [[:c "foo"]]}))))