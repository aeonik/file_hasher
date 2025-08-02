(ns aeonik.scratch
  (:require [aeonik.file-hasher :refer :all]
            [aeonik.parsers :refer :all]
            [injest.classical :refer [x>> =>>]])
  (:import (java.io BufferedReader FileReader)))

(def file-paths ["/opt/backup_nvme_recovery/backup_hdd_sha256sums.txt"
                 "/opt/backup_nvme_recovery/backup_nvme_sha256sums.txt"
                 "/opt/backup_nvme_recovery/2024-03-13/openssl_sha256sums_backup_hdd.txt"
                 "/opt/backup_nvme_recovery/2024-03-13/openssl_sha256sums_backup_nvme.txt"
                 "/opt/backup_nvme_recovery/2024-03-14/openssl_sha256sums_backup_nvme.txt"
                 "/opt/backup_nvme_recovery/2024-03-14/openssl_sha256sums_external_backup.txt"])

(require '[clojure.java.io :as io])
(require '[clojure.string :as str])

(defn determine-parse-fn [file-path]
  (if (str/includes? file-path "openssl")
    parse-openssl-line
    parse-sha256sum-line))
(defn process-file [file-path]
  (with-open [rdr (io/reader file-path)]
    (let [parse-fn (determine-parse-fn file-path)
          lines (line-seq rdr)]
      (x>> lines
           (map #(process-line % nil parse-fn))))))

(defn parse-all-files [file-paths]
  (let [results (pmap (fn [file-path]
                        (let [parse-fn (determine-parse-fn file-path)]
                          {file-path (process-file file-path)}))
                      file-paths)]
    (x>> results
         (apply merge))))
(comment (time (with-open [rdr (clojure.java.io/reader (first file-paths))]
                 (let [lines    (line-seq rdr)
                       parse-fn (determine-parse-fn (first file-paths))]
                   (x>> lines
                        (pmap #(process-line % nil parse-fn))
                        count)))))

(def file-hashes (parse-all-files file-paths))

(comment
  (time (x>> (line-seq (io/reader (first file-paths)))
             (map #(process-line % nil (determine-parse-fn (first file-paths))))
             count))

  (time (=>> (line-seq (io/reader (first file-paths)))
             (map #(process-line % nil (determine-parse-fn (first file-paths))))
             (map :hash)
             count))

  (time (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/2024-03-14/openssl_sha256sums_backup_nvme.txt"))
             (map #(process-line % nil (determine-parse-fn (first file-paths))))
             (map :hash)
             sort
             distinct
             set))
  (time (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/2024-03-14/openssl_sha256sums_external_backup.txt"))
             (map #(process-line % nil (determine-parse-fn (first file-paths))))
             (map :hash)
             sort
             distinct
             set))

  ;; Set difference between two sets
  (time (clojure.set/difference (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/2024-03-14/openssl_sha256sums_backup_nvme.txt"))
                                     (map #(process-line % nil (determine-parse-fn "openssl")))
                                     (map :hash)

                                     set)

                                (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/2024-03-14/openssl_sha256sums_external_backup.txt"))
                                     (map #(process-line % nil (determine-parse-fn "openssl")))
                                     (map :hash)

                                     set)

                                (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/2024-03-13/openssl_sha256sums_backup_hdd.txt"))
                                     (map #(process-line % nil (determine-parse-fn "openssl")))
                                     (map :hash)

                                     set)

                                (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/2024-03-13/openssl_sha256sums_backup_nvme.txt"))
                                     (map #(process-line % nil (determine-parse-fn "openssl")))
                                     (map :hash)

                                     set)

                                (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/backup_nvme_sha256sums.txt"))
                                     (map #(process-line % nil (determine-parse-fn "sha")))
                                     (map :hash)

                                     set)

                                (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/backup_hdd_sha256sums.txt"))
                                     (map #(process-line % nil (determine-parse-fn "sha")))
                                     (map :hash)
                                     set)

                                (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/test_hashes.txt"))
                                     (map #(process-line % nil (determine-parse-fn "sha")))
                                     (map :hash)
                                     set)))

  (time (clojure.set/difference
         (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/test_hashes.txt"))
              (map #(process-line % nil (determine-parse-fn "sha")))
              (map :hash)
              set)

         (=>> (line-seq (io/reader "/opt/backup_nvme_recovery/test_hashes2.txt"))
              (map #(process-line % nil (determine-parse-fn "sha")))
              (map :hash)
              set))))

;; Example usage
(def parsed-results-by-file (parse-all-files file-paths))
