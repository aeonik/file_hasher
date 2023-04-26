(ns aeonik.file-hasher
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.java.shell :as shell]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :as json])
  (:import (org.apache.commons.io FilenameUtils)
           (java.time LocalDateTime Instant ZoneId)
           (java.time.format DateTimeFormatter)
           (java.io BufferedWriter FileWriter)))


(def time-atom (atom (System/currentTimeMillis)))
(def config (slurp "resources/secrets.edn"))
(defn update-time []
  (let [current-time-ms (System/currentTimeMillis)
        local-date-time (LocalDateTime/ofInstant (Instant/ofEpochMilli current-time-ms)
                                                 (ZoneId/systemDefault))
        formatter       (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSS")
        formatted-time  (.format local-date-time formatter)]
    (print (str formatted-time " "))
    (reset! time-atom current-time-ms)))

(defn get-elapsed-time []
  (let [current-time-ms (System/currentTimeMillis)
        elapsed-time    (/ (- current-time-ms @time-atom) 1000.0)]
    (reset! time-atom current-time-ms)
    elapsed-time))

(def cli-options
  ;; An option with a required argument
  [["-d" "--directory DIR" "Directory Root"
    :id :directory
    :default (System/getProperty "user.home")
    :parse-fn #(str/trim %)
    :validate [#(not-empty %) "Directory path cannot be empty"]]
   ;; A non-idempotent option (:default is applied first)
   ["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :update-fn inc]                                         ; Prior to 0.4.1, you would have to use:
   ;; :assoc-fn (fn [m k _] (update-in m [k] inc))
   ;; A boolean option defaulting to nil
   ["-h" "--help"]])

(defn clear-terminal []
  (print "\033[2J\033[H")
  (flush))

(def base-dir (:base-dir config))
(def working-dir (:working-dir config))
;; Make directory for files if it doesn't already exist
;(io/make-parents (str base-dir "/opt/backup_nvme_recovery"))

(def smoothing-alpha 0.01)
(def average-time-per-file (atom 0))
(def remaining-files-rate (atom 0))
(def last-update-time (atom (System/currentTimeMillis)))

(defn strip-path-prefix [path prefix]
  (str/replace path (re-pattern (str (java.util.regex.Pattern/quote prefix) "(.*?)$")) "$1"))

(defn parse-line
  ([line] (parse-line line nil))
  ([line prefix]
   (let [[hash path] (str/split line #"\s+" 2)
         basename  (FilenameUtils/getBaseName path)
         extension (FilenameUtils/getExtension path)
         dirname   (FilenameUtils/getFullPathNoEndSeparator path)
         result    {:path      path
                    :dirname   dirname
                    :basename  basename
                    :extension (when (not-empty extension) (str "." extension))
                    :hash      hash}]
     (if prefix
       (let [stripped-path-prefix prefix
             stripped-path        (strip-path-prefix path prefix)]
         (assoc result :stripped-path-prefix stripped-path-prefix
                       :stripped-path stripped-path))
       result))))

(defmulti read-file-into-set
          (fn [file-path & args]
            (count args)))

(defmethod read-file-into-set 0
  [file-path]
  (let [lines (with-open [rdr (io/reader file-path)]
                (doall (line-seq rdr)))]
    ;(pp/pprint (str "Lines:" lines))                        ; Print lines for debugging
    (into #{} lines)))

(defmethod read-file-into-set 1
  [file-path num-lines]
  (let [lines (with-open [rdr (io/reader file-path)]
                (doall
                  (take num-lines (line-seq rdr))
                  ))]
    ;(pp/pprint (str "Lines:" lines))                        ; Print lines for debugging
    (into #{} lines)))

(def batch-size 10000)

(def db-spec
  {:subprotocol "postgresql"
   :subname     "//localhost:5432/file_hasher"
   :user        "file_hasher"
   :password    (:db-password config)})

(defn build-data-row [parsed-entry]
  {:path                 (:path parsed-entry)
   :hash                 (:hash parsed-entry)
   :dirname              (:dirname parsed-entry)
   :basename             (:basename parsed-entry)
   :stripped_path_prefix (:stripped-path-prefix parsed-entry)
   :stripped_path        (:stripped-path parsed-entry)
   :extension            (:extension parsed-entry)})

(defn build-data-rows [files prefix]
  (map #(build-data-row (parse-line % prefix)) files))

(defn insert-data [path hash dirname basename stripped-path-prefix stripped-path extension]
  (jdbc/with-db-connection [conn db-spec]
                           (jdbc/insert! conn :paths
                                         {:path                 path
                                          :hash                 hash
                                          :dirname              dirname
                                          :basename             basename
                                          :stripped_path_prefix stripped-path-prefix
                                          :stripped_path        stripped-path
                                          :extension            extension})))

(defn insert-data-batch [data batch-size]
  (doseq [chunk (partition-all batch-size data)]
    (jdbc/with-db-connection [conn db-spec]
                             (jdbc/insert-multi! conn :paths chunk))))

(defn load-files-into-db [files prefix batch-size]
  (let [data (map #(build-data-row (parse-line % prefix)) files)]
    (insert-data-batch data batch-size)))

(defn fetch-duplicate-paths []
  (println "Fetching duplicate paths...")
  (jdbc/with-db-connection [conn db-spec]
                           (jdbc/query conn
                                       ["SELECT p.hash, p.path, d.count
                  FROM paths AS p
                  JOIN (
                    SELECT hash, COUNT(hash) AS count
                    FROM paths
                    GROUP BY hash
                    HAVING COUNT(hash) > 1
                  ) AS d
                  ON p.hash = d.hash
                  ORDER BY d.count DESC, p.hash, p.path"])))

(defn save-results-to-file [results file-path]
  (with-open [writer (BufferedWriter. (FileWriter. file-path))]
    (doseq [row results]
      (.write writer (str (:hash row) " " (:path row) " " (:count row) "\n")))))

(defn export-duplicate-paths [output-file]
  (println "Exporting duplicate paths to file...")
  (let [results (fetch-duplicate-paths)]
    (save-results-to-file results output-file)))

;; Usage: (export-duplicate-paths "output.txt")

(defn exists-in-set? [item set comparator]
  (some #(comparator item %) set))

(defn custom-set-difference [set1 set2 comparator]
  (into #{} (filter #(not (exists-in-set? % set2 comparator)) set1)))

(defn compare-file-path-and-hash [a b]
  (and (= (:path a) (:path b))
       (= (:hash a) (:hash b))))

(defn find-missing-files [nvme-file-list hdd-file-list nvme-prefix hdd-prefix]
  (let [nvme-files      (read-file-into-set nvme-file-list nvme-prefix)
        hdd-files       (read-file-into-set hdd-file-list hdd-prefix)
        missing-in-nvme (set/difference hdd-files nvme-files)
        missing-in-hdd  (set/difference nvme-files hdd-files)]
    {:missing-in-nvme missing-in-nvme
     :missing-in-hdd  missing-in-hdd}))

(defn get-basename [path]
  (FilenameUtils/getBaseName path))

(defn get-dirname [path]
  (FilenameUtils/getFullPathNoEndSeparator path))

(defn insert-before-extension [filename text]
  (let [[_ basename extension] (re-matches #"(.*?)(\.[^.]+)?$" filename)]
    (str basename text extension)))

(defn generate-paths [base-dir]
  (let [basename               (get-basename base-dir)
        dirname                (get-dirname base-dir)
        working-dir            (working-dir)
        hashed-file-path       (str working-dir "/" basename "_sha256sums.txt")
        hashed-file-error-path (insert-before-extension hashed-file-path "_errors")
        file-list-path         (str working-dir "/" basename "_file_list.txt")
        file-list-error-path   (insert-before-extension file-list-path "_errors")]
    {:base-dir               base-dir
     :working-dir            working-dir
     :hashed-file-path       hashed-file-path
     :hashed-file-error-path hashed-file-error-path
     :file-list-path         file-list-path
     :file-list-error-path   file-list-error-path}))


(def paths (generate-paths base-dir))

(defn file-set []
  (read-file-into-set (:file-list-path paths)))

(defn hashed-file-set []
  (read-file-into-set (:hashed-file-path paths)))

(defn extract-file-paths [hashed-set]
  (into #{} (map (fn [line]
                   (second (str/split line #"\s{2,}" 2)))
                 hashed-set)))



;(def ^set hashed-file-paths (extract-file-paths hashed-file-set))
(defn hashed-file-paths []
  (extract-file-paths (hashed-file-set)))

;(def ^set unhashed-files (set/difference file-set hashed-file-paths))
(defn unhashed-files []
  (set/difference (file-set) (hashed-file-paths)))


(defn hash-file [file-path]
  (let [{:keys [out err exit]} (shell/sh "sha256sum" file-path)]
    (if (= exit 0)
      out
      (do
        (spit (:hashed-file-path paths) (str out "  " file-path "\n") :append true)
        nil)
      )))


(defn greet
  "Callable entry point to the application."
  [data]
  (println (str "Hello, " (or (:name data) "World") "!")))

(defn exponential-moving-average [current-average new-value weight]
  (+ (* weight new-value) (* (- 1 weight) current-average)))

(defn update-progress [file-count hashed-count remaining-count]
  (let [percentage-complete           (float (* 100 (/ (- file-count remaining-count) file-count)))
        current-time                  (System/currentTimeMillis)
        time-since-last-update        (- current-time @last-update-time)
        raw-remaining-files-rate      (if (zero? time-since-last-update) @remaining-files-rate (/ 1 time-since-last-update)) ; files per millisecond
        smoothed-remaining-files-rate (exponential-moving-average @remaining-files-rate raw-remaining-files-rate smoothing-alpha)
        remaining-time                (long (* remaining-count (/ 1 smoothed-remaining-files-rate))) ; in milliseconds
        remaining-hours               (float (/ remaining-time (* 1000 60 60)))] ; convert to hours
    (print (str "Files remaining: " remaining-count ", "
                "Percentage complete: " (format "%.2f" (float percentage-complete)) "%, "
                "Estimated time remaining: " (format "%.2f" remaining-hours) " hours \r"))
    (reset! last-update-time current-time)
    (reset! remaining-files-rate smoothed-remaining-files-rate)
    (flush)))

(defn hash-and-append-result [file-path]
  (let [hash-result (hash-file file-path)]
    (when hash-result
      (spit (:hashed-file-path paths) hash-result :append true)
      hash-result)))

(defn hash-files [file-set hashed-file-paths unhashed-files]
  (let [file-set-count            (count file-set)
        starting-hashed-set-count (count hashed-file-paths)
        unhashed-files-remaining  (atom (count unhashed-files))]

    (println "Total files:" file-set-count)
    (println "Starting Hashed files:" starting-hashed-set-count)
    (println "Starting Unhashed files:" @unhashed-files-remaining)

    (doall (pmap (fn [file-path]
                   (let [hash-result (hash-and-append-result file-path)]
                     (when hash-result
                       (swap! unhashed-files-remaining dec)
                       (update-progress file-set-count starting-hashed-set-count @unhashed-files-remaining))))
                 unhashed-files))))

(defn process-files [args]
  (println "Starting process-files...")
  (println "Loading data from disk... ")
  (let [{:keys [options]} (parse-opts args cli-options)
        base-dir            (:directory options)
        paths               (generate-paths base-dir)
        nvme-file-hash-list (str (:working-dir paths) "/" "backup_nvme_sha256sums.txt")
        hdd-file-hash-list  (str (:working-dir paths) "/" "backup_hdd_sha256sums.txt")
        nvme-prefix         (:prefix2 config)
        hdd-prefix          (:prefix1 config)
        missing-nvme-file   (str (:working-dir paths) "/" "missing_in_nvme.txt")
        missing-hdd-file    (str (:working-dir paths) "/" "missing_in_hdd.txt")
        nvme-files          (read-file-into-set nvme-file-hash-list)
        hdd-files           (read-file-into-set hdd-file-hash-list)]

    (println "Base directory:" base-dir)
    (println "Working directory:" (:working-dir paths))
    (println "Hashed file path:" (:hashed-file-path paths))
    (println "Hashed file error path:" (:hashed-file-error-path paths))
    (println "File list path:" (:file-list-path paths))
    (println "File list error path:" (:file-list-error-path paths))
    (println (str "Data Loaded into sets in " (get-elapsed-time) " seconds"))

    (println "Entering main parsing loops...")
    (println "Begin processing hdd-files...")
    (doseq [entry hdd-files]
      (try
        (let [parsed-entry (parse-line entry hdd-prefix)]
          ;(println (str parsed-entry))
          ;(println (str (:path parsed-entry)))
          ;(println (str (:hash parsed-entry)))
          ;(println (str (:dirname parsed-entry)))
          ;(println (str (:basename parsed-entry)))
          ;(println (str (:stripped-path-prefix parsed-entry)))
          ;(println (str (:stripped-path parsed-entry)))
          )
        (catch Exception e
          (println "Error processing entry:" entry)
          (println "Exception:" e))))
    (println (str "Elapsed time " (get-elapsed-time) " seconds"))
    (println "Begin processing nvme-files...")

    (println (str "Elapsed time " (get-elapsed-time) " seconds"))
    (comment
      (println "Loading database with hdd-files... ")
      (load-files-into-db hdd-files hdd-prefix batch-size))

    (println (str "Elapsed time " (get-elapsed-time) " seconds"))
    (comment
      (println "Loading database with nvme-files... ")
      (load-files-into-db nvme-files nvme-prefix batch-size))

    (println (str "Elapsed time " (get-elapsed-time) " seconds"))
    (println "Done processing files!")
    (comment (let [missing-files (find-missing-files nvme-file-hash-list hdd-file-hash-list nvme-prefix hdd-prefix)]
               (println "Missing in NVMe:")
               (pp/pprint (:missing-in-nvme missing-files))
               (spit missing-nvme-file (str/join "\n" (map str (:missing-in-nvme missing-files))))
               (println "Missing in HDD:")
               (pp/pprint (:missing-in-hdd missing-files))
               (spit missing-hdd-file (str/join "\n" (map str (:missing-in-hdd missing-files))))))
    (comment (hash-files
               (file-set)
               (hashed-file-paths)
               (unhashed-files)))))

(defn run-export-duplicate-paths [args]
  (let [{:keys [options]} (parse-opts args cli-options)
        base-dir    (:directory options)
        paths       (generate-paths base-dir)
        output-file (str (:working-dir paths) "/" "duplicate_files_detected.txt")]

    (export-duplicate-paths output-file)))

(defn export-data-rows [output-file data-rows]
  (with-open [writer (io/writer output-file)]
    (doseq [row data-rows]
      (.write writer (json/generate-string row))
      (.write writer "\n"))))



(defn run-export-data-rows [output-file data-rows]
  (export-data-rows output-file data-rows))

;; Usage: (process-files args)


(defn -main
  [& args]
  (update-time)
  (println)
  (println "file_hasher starting...")

  (comment run-export-duplicate-paths args)
  (comment (process-files args))

  (let [{:keys [options]} (parse-opts args cli-options)
        base-dir            (:directory options)
        paths               (generate-paths base-dir)
        nvme-file-hash-list (str (:working-dir paths) "/" "backup_nvme_sha256sums.txt")
        hdd-file-hash-list  (str (:working-dir paths) "/" "backup_hdd_sha256sums.txt")
        nvme-prefix         (:prefix2 config)
        hdd-prefix          (:prefix1 config)
        missing-nvme-file   (str (:working-dir paths) "/" "missing_in_nvme.txt")
        missing-hdd-file    (str (:working-dir paths) "/" "missing_in_hdd.txt")
        nvme-files          (read-file-into-set nvme-file-hash-list)
        hdd-files           (read-file-into-set hdd-file-hash-list)
        output-file         (str (:working-dir paths) "/" "data_rows.json")]


    (export-data-rows output-file (concat (build-data-rows nvme-files nvme-prefix)
                                          (build-data-rows hdd-files hdd-prefix))))
  (println "Done!"))