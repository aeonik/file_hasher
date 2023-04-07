(ns aeonik.file-hasher
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.java.shell :as shell]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.tools.cli :refer [parse-opts]]))


(def cli-options
  ;; An option with a required argument
  [["-d" "--directory DIR" "Directory Root"
    :default (System/getProperty "user.home")
    :parse-fn #(str/trim %)
    :validate [#(not-empty %) "Directory path cannot be empty"]]
   ;; A non-idempotent option (:default is applied first)
   ["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :update-fn inc] ; Prior to 0.4.1, you would have to use:
   ;; :assoc-fn (fn [m k _] (update-in m [k] inc))
   ;; A boolean option defaulting to nil
   ["-h" "--help"]])

(def base-dir "/run/media/dave/backup_hdd")
(def working-dir "/opt/backup_nvme_recovery")
;; Make directory for files if it doesn't already exist
;(io/make-parents (str base-dir "/opt/backup_nvme_recovery"))

(def hashed-file-path "/opt/backup_nvme_recovery/sha256sums_backup_hdd.txt")
(def error-file-path "/opt/backup_nvme_recovery/sha256sums_backup_hdd_errors.txt")
(def file-list-path "/opt/backup_nvme_recovery/backup_hdd_file_list.txt")
(def alpha 0.01)
(def smoothing-alpha 0.01)
(def average-time-per-file (atom 0))
(def remaining-files-rate (atom 0))
(def last-update-time (atom (System/currentTimeMillis)))
(defn read-file-into-set [file-path]
  (let [lines (with-open [rdr (io/reader file-path)]
                (doall (line-seq rdr)))]
    (into #{} lines)))

(def ^set file-set (read-file-into-set file-list-path))

(def ^set hashed-file-set (read-file-into-set hashed-file-path))

(defn extract-file-paths [hashed-set]
  (into #{} (map (fn [line]
                   (second (str/split line #"\s{2,}" 2)))
                 hashed-set)))

(def ^set hashed-file-paths (extract-file-paths hashed-file-set))

(def ^set unhashed-files (set/difference file-set hashed-file-paths))

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

(defn exponential-moving-average [current-average new-value weight]
  (+ (* weight new-value) (* (- 1 weight) current-average)))

(defn update-progress [file-count hashed-count remaining-count]
  (let [percentage-complete (float (* 100 (/ (- file-count remaining-count) file-count)))
        current-time (System/currentTimeMillis)
        time-since-last-update (- current-time @last-update-time)
        raw-remaining-files-rate (if (zero? time-since-last-update) @remaining-files-rate (/ 1 time-since-last-update)) ; files per millisecond
        smoothed-remaining-files-rate (exponential-moving-average @remaining-files-rate raw-remaining-files-rate smoothing-alpha)
        remaining-time (long (* remaining-count (/ 1 smoothed-remaining-files-rate))) ; in milliseconds
        remaining-hours (float (/ remaining-time (* 1000 60 60)))] ; convert to hours
    (print (str "Files remaining: " remaining-count ", "
                "Percentage complete: " (format "%.2f" (float percentage-complete)) "%, "
                "Estimated time remaining: " (format "%.2f" remaining-hours) " hours \r"))
    (reset! last-update-time current-time)
    (reset! remaining-files-rate smoothed-remaining-files-rate)
    (flush)))

(defn hash-and-append-result [file-path]
  (let [hash-result (hash-file file-path)]
    (when hash-result
      (spit hashed-file-path hash-result :append true)
      hash-result)))

(defn -main
  [& args]
  (greet {:name (first args)})

  (let [file-set-count (count file-set)
        starting-hashed-set-count (count hashed-file-paths)
        unhashed-files-remaining (atom (count unhashed-files))]

    (println "Total files:" file-set-count)
    (println "Starting Hashed files:" starting-hashed-set-count)
    (println "Starting Unhashed files:" @unhashed-files-remaining)

    (doall (pmap (fn [file-path]
                   (let [hash-result (hash-and-append-result file-path)]
                     (when hash-result
                       (swap! unhashed-files-remaining dec)
                       (update-progress file-set-count starting-hashed-set-count @unhashed-files-remaining))))
                 unhashed-files))))

