(ns aeonik.inventory.scanner-test
  (:require [clojure.test :refer [deftest is testing]]
            [aeonik.inventory.scanner :as scanner])
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

(deftest scans-files-directories-and-symlinks
  (let [root (Files/createTempDirectory
              "file-hasher-test-"
              (make-array FileAttribute 0))]
    (try
      (let [directory (Files/createDirectory
                       (.resolve root "nested")
                       (make-array FileAttribute 0))
            file (.resolve directory "essay.txt")
            newline-file (.resolve root "line\nbreak.txt")
            link (.resolve root "essay-link")
            _ (Files/write file
                           (.getBytes "cosmology\n" StandardCharsets/UTF_8)
                           (make-array java.nio.file.OpenOption 0))
            _ (Files/write newline-file
                           (.getBytes "religion\n" StandardCharsets/UTF_8)
                           (make-array java.nio.file.OpenOption 0))
            _ (Files/createSymbolicLink
               link (.getFileName file) (make-array FileAttribute 0))
            batches (atom [])
            counts (scanner/walk!
                    {:root root
                     :scan-id "scan:test"
                     :root-id "root:test"
                     :batch-size 2}
                    #(swap! batches into %))
            observations @batches
            by-path (into {} (map (juxt :relative_path identity)) observations)]
        (testing "all entry kinds are observations"
          (is (= 5 (:observations counts)))
          (is (= "directory" (:entry_type (get by-path ""))))
          (is (= "directory" (:entry_type (get by-path "nested"))))
          (is (= "regular_file" (:entry_type
                                 (get by-path "nested/essay.txt"))))
          (is (= "symlink" (:entry_type (get by-path "essay-link")))))
        (testing "regular files are hashed and stability checked"
          (let [observation (get by-path "nested/essay.txt")]
            (is (= "stable" (:status observation)))
            (is (= "SHA-256" (:hash_algorithm observation)))
            (is (= 64 (count (:hash_digest observation))))
            (is (string? (:content_id observation)))))
        (testing "paths are not line-oriented"
          (is (bytes? (:relative_path_bytes
                       (get by-path "nested/essay.txt"))))
          (is (= "stable" (:status (get by-path "line\nbreak.txt"))))))
      (finally
        (delete-tree! root)))))
