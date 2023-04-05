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
(def alpha 0.1)
(def average-time-per-file (atom 0))
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

(defn update-progress [file-count hashed-count remaining-count elapsed-time]
  (let [percentage-complete (float (* 100 (/ (- file-count remaining-count) file-count)))
        avg-time-per-file (/ elapsed-time hashed-count)
        remaining-time (* avg-time-per-file remaining-count)]
    (print (str "Files remaining: " remaining-count ", "
                "Percentage complete: " (format "%.2f" (float percentage-complete)) "%, "
                "Estimated time remaining: " (format "%.2f" (float (/ remaining-time (* 1000 60 60)))) " hours \r"))
    (flush)))

(defn hash-and-update-progress [file]
  (let [start-time (System/currentTimeMillis)
        hash-result (hash-file file)
        end-time (System/currentTimeMillis)
        time-taken (- end-time start-time)]
    (when hash-result
      (spit hashed-file-path hash-result :append true))
    {:hash-result hash-result
     :time-taken time-taken}))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (greet {:name (first args)})

  (let [file-set-count (count file-set)
        starting-hashed-set-count (count hashed-file-paths)
        unhashed-files-remaining (atom (count unhashed-files))
        start-time (System/currentTimeMillis)]

    (println "Total files:" file-set-count)
    (println "Starting Hashed files:" starting-hashed-set-count)
    (println "Starting Unhashed files:" @unhashed-files-remaining)

    (doall (pmap (fn [file]
                   (let [{:keys [hash-result time-taken]} (hash-and-update-progress file)
                         current-time (System/currentTimeMillis)
                         elapsed-time (- current-time start-time)
                         files-processed (abs (+ starting-hashed-set-count (- @unhashed-files-remaining)))]

                     (when hash-result
                       (swap! unhashed-files-remaining dec)
                       (swap! average-time-per-file (fn [current-average]
                                                      (exponential-moving-average current-average time-taken alpha)))
                       (update-progress file-set-count files-processed @unhashed-files-remaining @average-time-per-file))))
                 unhashed-files))))
