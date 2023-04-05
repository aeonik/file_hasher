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

  (let [file-set-count (count file-set)
        starting-hashed-set-count (count hashed-file-paths)
        unhashed-files-remaining (atom (count unhashed-files))
        start-time (System/currentTimeMillis)
        time-queue (atom [])
        n 10]

    ;; Number of total files
    (println "Total files:" file-set-count)
    ;; Number of hashed files
    (println "Starting Hashed files:" starting-hashed-set-count)
    (println "Starting Unhashed files:" @unhashed-files-remaining)

    (doseq [file unhashed-files]
      (let [hash-result (hash-file file)
            current-time (System/currentTimeMillis)
            elapsed-time (- current-time start-time)
            files-processed (abs (+ starting-hashed-set-count (- @unhashed-files-remaining)))
            percentage-complete (float (* 100 (/ files-processed file-set-count)))
            _ (swap! time-queue conj current-time)
            _ (when (> (count @time-queue) n) (swap! time-queue pop))
            avg-time-per-file (/ (reduce + (map - (rest @time-queue) @time-queue)) (min n files-processed))
            remaining-time (* avg-time-per-file @unhashed-files-remaining)]

        (when hash-result
          (spit hashed-file-path hash-result :append true)
          (swap! unhashed-files-remaining dec)
          (print (str "Files remaining: " (abs (+ starting-hashed-set-count (- @unhashed-files-remaining))) ", "
                      "Percentage complete: " (format "%.2f" (float (* 100 (/ (- file-set-count @unhashed-files-remaining) file-set-count)))) "%, "
                      "Estimated time remaining: " (format "%.4f" (double (/ remaining-time (* 1000 60 60)))) " hours \r"))
          (flush))

        ))))

