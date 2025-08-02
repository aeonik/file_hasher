(ns aeonik.file-hasher-test
  (:require [clojure.test :refer :all]
            [aeonik.file-hasher :refer :all]
            [aeonik.parsers :refer :all]))

(deftest test-strip-path-prefix
  (testing "strip-path-prefix"
    (is (= (strip-path-prefix "/prefix/some/path/file.txt" "/prefix/")
           "some/path/file.txt" ))))

(deftest test-parse-line
  (testing "parse-line"
    (let [parsed (parse-sha256sum-line "abcdef123456  /path/to/file.txt")
          result (apply parse-line parsed)]
      (is (= (:hash result)
             "abcdef123456"))
      (is (= (:path result)
             "/path/to/file.txt"))
      (is (= (:basename result)
             "file"))
      (is (= (:extension result)
             ".txt"))
      (is (= (:dirname result)
             "/path/to")))))

(deftest test-insert-before-extension
  (testing "insert-before-extension"
    (is (= "file_inserted.txt" (insert-before-extension "file.txt" "_inserted")))
    (is (= "file_inserted" (insert-before-extension "file" "_inserted")))))

(deftest test-generate-paths
  (testing "generate-paths"
    (let [base-dir "test/dir"
          paths (generate-paths base-dir)]
      (is (= base-dir (:base-dir paths)))
      (is (= "/opt/backup_nvme_recovery" (:working-dir paths)))
      (is (= "/opt/backup_nvme_recovery/dir_sha256sums.txt" (:hashed-file-path paths)))
      (is (= "/opt/backup_nvme_recovery/dir_file_list.txt" (:file-list-path paths))))))

(deftest test-exponential-moving-average
  (testing "exponential-moving-average"
    (is (= (exponential-moving-average 100.0 0.0 0.5)
           50.0))
    (is (= (exponential-moving-average 100.0 50.0 0.5)
           75.0))
    (is (= (exponential-moving-average 100.0 90.0 0.5)
           95.0))))

(deftest test-hash-file
  (testing "hash-file"
    (let [hash-result (hash-file "resources/test-file.txt")
          expected-hash "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  resources/test-file.txt\n"]
      (is (= hash-result
             expected-hash)))))

(run-tests 'aeonik.file-hasher-test)