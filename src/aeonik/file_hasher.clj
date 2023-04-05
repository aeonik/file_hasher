(ns aeonik.file-hasher
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.java.shell :as shell]
            [clojure.pprint :as pp]
            [clojure.set :as set]))

(def hashed-file-path "/opt/backup_nvme_recovery/sha256sums_backup_hdd.txt")
(def error-file-path "/opt/backup_nvme_recovery/sha256sums_backup_hdd_errors.txt")
(def file-list-path "/opt/backup_nvme_recovery/backup_hdd_file_list.txt")

(defn read-file-into-set [file-path]
  (let [lines (with-open [rdr (io/reader file-path)]
                (doall (line-seq rdr)))]
    (into #{} lines)))

(def file-set (read-file-into-set file-list-path))

(def hashed-file-set (read-file-into-set hashed-file-path))

(defn extract-file-paths [hashed-set]
  (into #{} (map (fn [line]
                   (second (str/split line #"\s{2,}" 2)))
                 hashed-set)))

(def hashed-file-paths (extract-file-paths hashed-file-set))

(def unhashed-files (set/difference file-set hashed-file-paths))

(defn hash-file [file-path]
  (let [{:keys [out err exit]} (shell/sh "sha256sum" file-path)]
    (if (= exit 0)
      out
      (do
        (spit error-file-path err :append true)
        nil))))


(defn greet
  "Callable entry point to the application."
  [data]
  (println (str "Hello, " (or (:name data) "World") "!")))


(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (greet {:name (first args)})

  (def file-set-count (count file-set))
  (def starting-hashed-set-count (count hashed-file-paths))
  (def unhashed-files-remaining (atom (count unhashed-files)))

  ;; Number of total files
  (println "Total files:" file-set-count)
  ;; Number of hashed files
  (println "Hashed files:" starting-hashed-set-count)
  (println "Unhashed files to go:" @unhashed-files-remaining)

  ;; Process unhashed files
  (doseq [file unhashed-files]
    (let [hash-result (hash-file file)]
      (when hash-result
        (spit hashed-file-path hash-result :append true)
        (swap! unhashed-files-remaining dec)
        (print (str "Files remaining: " (abs (+ starting-hashed-set-count (- @unhashed-files-remaining)))
                    " Percentage complete: " (float (* 100 (/ (- file-set-count @unhashed-files-remaining) file-set-count))) "%\r"))
        (flush))))

  )