(ns aeonik.inventory.xtdb-test
  (:require [clojure.test :refer [deftest is testing]]
            [aeonik.inventory.service :as service]
            [aeonik.inventory.xtdb :as inventory-xtdb]
            [xtdb.node :as xtn])
  (:import (java.nio.charset StandardCharsets)
           (java.nio.file Files Path)
           (java.nio.file.attribute FileAttribute)))

(defn- delete-tree! [^Path root]
  (when (Files/exists root (make-array java.nio.file.LinkOption 0))
    (with-open [paths (Files/walk
                       root
                       (make-array java.nio.file.FileVisitOption 0))]
      (doseq [path (reverse (sort-by #(.getNameCount ^Path %)
                                     (iterator-seq (.iterator paths))))]
        (Files/deleteIfExists path)))))

(deftest persists-a-complete-scan-in-xtdb
  (let [root (Files/createTempDirectory
              "file-hasher-xtdb-test-"
              (make-array FileAttribute 0))]
    (try
      (Files/write (.resolve root "note.txt")
                   (.getBytes "intersection\n" StandardCharsets/UTF_8)
                   (make-array java.nio.file.OpenOption 0))
      (with-open [node (xtn/start-node)]
        (let [result (service/run-scan!
                      (inventory-xtdb/store node)
                      {:root root
                       :root-id "root:test"
                       :host "test-host"
                       :batch-size 1})
              scan (inventory-xtdb/scan-by-id node (:scan_id result))
              observations
              (inventory-xtdb/observations-for-scan
               node (:scan_id result))]
          (testing "completion is asserted only after observations persist"
            (is (= "complete" (:status result)))
            (is (= "complete" (:status scan)))
            (is (= 2 (:observations scan))))
          (testing "the root directory and file were both stored"
            (is (= #{"" "note.txt"}
                   (set (map :relative-path observations))))
            (is (= #{"directory" "regular_file"}
                   (set (map :entry-type observations)))))))
      (finally
        (delete-tree! root)))))
