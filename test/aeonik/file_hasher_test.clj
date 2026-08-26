(ns aeonik.file-hasher-test
  (:require [clojure.test :refer [deftest is testing]]
            [aeonik.parsers :as parsers]))

(deftest parses-sha256sum-lines
  (testing "legacy manifests remain importable"
    (let [[hash path]
          (parsers/parse-sha256sum-line
           "abcdef123456  /path with spaces/file.txt")
          parsed (parsers/parse-line hash path)]
      (is (= "abcdef123456" (:hash parsed)))
      (is (= "/path with spaces/file.txt" (:path parsed)))
      (is (= "file" (:basename parsed)))
      (is (= ".txt" (:extension parsed)))
      (is (= "/path with spaces" (:dirname parsed))))))
