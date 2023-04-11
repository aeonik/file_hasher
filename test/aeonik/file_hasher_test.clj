(ns aeonik.file-hasher-test
  (:require [clojure.test :refer :all]
            [aeonik.file-hasher :refer :all]))

(deftest test-strip-path-prefix
  (testing "strip-path-prefix"
    (is (= "some/path/file.txt" (strip-path-prefix "/prefix/some/path/file.txt" "/prefix")))))

(deftest test-parse-line
  (testing "parse-line"
    (let [result (parse-line "abcdef123456  /path/to/file.txt")]
      (is (= "abcdef123456" (:hash result)))
      (is (= "/path/to/file.txt" (:path result)))
      (is (= "file" (:basename result)))
      (is (= ".txt" (:extension result)))
      (is (= "/path/to" (:dirname result))))))

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
    (is (= 50.0 (exponential-moving-average 100.0 0.0 0.5)))
    (is (= 75.0 (exponential-moving-average 100.0 50.0 0.5)))
    (is (= 95.0 (exponential-moving-average 100.0 90.0 0.5)))))

(deftest test-update-progress
  (testing "update-progress"
    (with-out-str (binding [*out* (java.io.StringWriter.)]
                    (update-progress 100 50 50)
                    (.toString *out*)))
    (is (re-find #"Files remaining: 50" *out*)
        "Expected output to contain remaining files count")
    (is (re-find #"Percentage complete: 50.00%" *out*)
        "Expected output to contain percentage complete")
    (is (re-find #"Estimated time remaining: \d+\.\d+ hours" *out*)
        "Expected output to contain estimated time remaining")))

(deftest test-hash-file
  (testing "hash-file"
    (let [hash-result (hash-file "resources/test-file.txt")
          expected-hash "76e6896a90f764e1f0d9e243d1e63a7c6a0a6d8c3aeb08d543c3d2f2a1dd9a3d  "]
      (is (= expected-hash hash-result)))))

(run-tests 'aeonik.file-hasher-test)