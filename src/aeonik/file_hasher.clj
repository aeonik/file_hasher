(ns aeonik.file-hasher
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.java.shell :as shell]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.jdbc :as jdbc])
  (:import (org.apache.commons.io FilenameUtils))
  )


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

(def base-dir "/run/media/dave/backup_nvme")
(def working-dir "/opt/backup_nvme_recovery")
;; Make directory for files if it doesn't already exist
;(io/make-parents (str base-dir "/opt/backup_nvme_recovery"))

(def smoothing-alpha 0.01)
(def average-time-per-file (atom 0))
(def remaining-files-rate (atom 0))
(def last-update-time (atom (System/currentTimeMillis)))

(defn strip-path-prefix [path prefix]
  (str/replace path (re-pattern (str (java.util.regex.Pattern/quote prefix) "(.*?)$")) "$1"))

(defmulti read-file-into-set
          (fn [file-path & args]
            (count args)))

(defmethod read-file-into-set 0
  [file-path]
  (let [lines (with-open [rdr (io/reader file-path)]
                (doall (line-seq rdr)))]
    (into #{} lines)))

(defmethod read-file-into-set 1
  [file-path num-lines]
  (let [lines (with-open [rdr (io/reader file-path)]
                (doall
                  (take num-lines (line-seq rdr))
                  ))]
    (into #{} lines)))

(defmulti parse-line (fn [line & args] (count args)))

(defmethod parse-line 0
  [line]
  (let [[hash path] (str/split line #"\s+" 2)
        basename (FilenameUtils/getBaseName path)
        extension (FilenameUtils/getExtension path)
        dirname (FilenameUtils/getFullPathNoEndSeparator path)]
    {:path      path
     :dirname   dirname
     :basename  basename
     :extension (when (not-empty extension) (str "." extension))
     :hash      hash}))

(defmethod parse-line 1
  [line prefix]
  (let [result               (parse-line line)
        path                 (:path result)
        stripped-path-prefix prefix
        basename             (:basename result)
        stripped-path        (strip-path-prefix path prefix)]
    (assoc result :stripped-path-prefix stripped-path-prefix
                  :stripped-path stripped-path)))

(def db-spec
  {:subprotocol "postgresql"
   :subname     "//localhost:5432/file_hasher"
   :user        "file_hasher"
   :password    "123123123"})

(defn insert-data [path hash dirname basename stripped-path-prefix stripped-path]
  (jdbc/with-db-connection [conn db-spec]
                           (jdbc/insert! conn :paths
                                         {:path                 path
                                          :hash                 hash
                                          :dirname              dirname
                                          :basename             basename
                                          :stripped_path_prefix stripped-path-prefix
                                          :stripped_path        stripped-path})))



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
        missing-in-nvme (clojure.set/difference hdd-files nvme-files)
        missing-in-hdd  (clojure.set/difference nvme-files hdd-files)]
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
        working-dir            "/opt/backup_nvme_recovery"
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
      ((do
         (spit (generate-paths :hashed-file-error-path) err :append true)
         nil)))))

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

(defn -main
  [& args]
  (let [{:keys [options]} (parse-opts args cli-options)
        base-dir            (:directory options)
        paths               (generate-paths base-dir)
        nvme-file-hash-list (str (:working-dir paths) "/" "backup_nvme_sha256sums.txt")
        hdd-file-hash-list  (str (:working-dir paths) "/" "backup_hdd_sha256sums.txt")
        nvme-prefix         "/mnt/nvme1"
        hdd-prefix          "/run/media/dave/backup_hdd/backup_nvme"
        missing-nvme-file   (str (:working-dir paths) "/" "missing_in_nvme.txt")
        missing-hdd-file    (str (:working-dir paths) "/" "missing_in_hdd.txt")
        nvme-files          (read-file-into-set nvme-file-hash-list 10)
        hdd-files           (read-file-into-set hdd-file-hash-list 10)
        ]


    (println "Base directory:" base-dir)
    (println "Working directory:" (:working-dir paths))
    (println "Hashed file path:" (:hashed-file-path paths))
    (println "Hashed file error path:" (:hashed-file-error-path paths))
    (println "File list path:" (:file-list-path paths))
    (println "File list error path:" (:file-list-error-path paths))
    (println "Smoothing alpha:" smoothing-alpha)


    (doseq [entry hdd-files]
      (let [parsed-entry (parse-line entry hdd-prefix)]
        (println parsed-entry)
        (println (:path parsed-entry))
        (println (:hash parsed-entry))
        (println (:dirname parsed-entry))
        (println (:basename parsed-entry))
        (println (:stripped-path-prefix parsed-entry))
        (println (:stripped-path parsed-entry))))

    (doseq [entry nvme-files]
      (let [parsed-entry (parse-line entry nvme-prefix)]
        (pp/pprint parsed-entry)
        (println (:path parsed-entry))
        (println (:hash parsed-entry))
        (println (:dirname parsed-entry))
        (println (:basename parsed-entry))
        (println (:stripped-path-prefix parsed-entry))
        (println (:stripped-path parsed-entry))))

    (comment (doseq [entry hdd-files]
               (let [parsed-entry (parse-line entry)]
                 (insert-data (:path parsed-entry)
                              (:hash parsed-entry)
                              (:dirname parsed-entry)
                              (:basename parsed-entry)
                              (:stripped-path-prefix parsed-entry)
                              (:stripped-path parsed-entry))))

             (doseq [entry nvme-files]
               (let [parsed-entry (parse-line entry)]
                 (insert-data (:path parsed-entry)
                              (:hash parsed-entry)
                              (:dirname parsed-entry)
                              (:basename parsed-entry)
                              (:stripped-path-prefix parsed-entry)
                              (:stripped-path parsed-entry)))))


    ; Use paths map for the rest of the program
    (greet {:name (first args)})

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