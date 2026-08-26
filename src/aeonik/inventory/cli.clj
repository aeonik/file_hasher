(ns aeonik.inventory.cli
  (:require [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [aeonik.inventory.service :as service]
            [aeonik.inventory.xtdb :as inventory-xtdb]))

(defn- parse-port [value]
  (parse-long value))

(defn- env [name fallback]
  (or (System/getenv name) fallback))

(def connection-options
  [[nil "--host HOST" "XTDB host"
    :default (env "FILE_HASHER_XTDB_HOST" "127.0.0.1")]
   [nil "--port PORT" "XTDB PostgreSQL-wire port"
    :default (parse-port (env "FILE_HASHER_XTDB_PORT" "5432"))
    :parse-fn parse-port
    :validate [pos? "Port must be positive"]]
   [nil "--database DATABASE" "XTDB database"
    :default (env "FILE_HASHER_XTDB_DATABASE" "xtdb")]
   [nil "--user USER" "XTDB user"
    :default (System/getenv "FILE_HASHER_XTDB_USER")]
   ["-h" "--help" "Show help"]])

(def scan-options
  (into
   [[nil "--root ROOT" "Explicit filesystem root to scan"]
    [nil "--root-id ID" "Stable operator-assigned identity for this root"]
    [nil "--same-filesystem"
     "Stop at nested filesystem boundaries (default: traverse them)"]
    [nil "--batch-size N" "Observations per XTDB transaction"
     :default 1000
     :parse-fn parse-long
     :validate [pos? "Batch size must be positive"]]]
   connection-options))

(defn- connection-config [options]
  {:host (:host options)
   :port (:port options)
   :database (:database options)
   :user (:user options)
   :password (System/getenv "FILE_HASHER_XTDB_PASSWORD")})

(defn- usage [summary]
  (str/join
   "\n"
   ["file_hasher XTDB 2 inventory"
    ""
    "Commands:"
    "  status"
    "  scan --root PATH [--root-id ID] [--same-filesystem]"
    ""
    "Options:"
    summary]))

(defn- ensure-valid-options! [{:keys [errors summary options]} require-root?]
  (when (seq errors)
    (throw (ex-info (str/join "\n" errors) {:errors errors})))
  (when (:help options)
    (println (usage summary)))
  (when (and require-root?
             (not (:help options))
             (str/blank? (:root options)))
    (throw (ex-info "--root is required for scan" {})))
  options)

(defn- run-status! [args]
  (let [parsed (parse-opts args connection-options)
        options (ensure-valid-options! parsed false)]
    (when-not (:help options)
      (with-open [client (inventory-xtdb/client
                          (connection-config options))]
        (prn (inventory-xtdb/status client))))))

(defn- run-scan! [args]
  (let [parsed (parse-opts args scan-options)
        options (ensure-valid-options! parsed true)]
    (when-not (:help options)
      (with-open [client (inventory-xtdb/client
                          (connection-config options))]
        (prn
         (service/run-scan!
          (inventory-xtdb/store client)
          {:root (:root options)
           :root-id (:root-id options)
           :batch-size (:batch-size options)
           :same-filesystem? (:same-filesystem options)}))))))

(defn -main [& args]
  (let [[command & command-args] args]
    (case command
      "status" (run-status! command-args)
      "scan" (run-scan! command-args)
      (println
       (usage
        (:summary (parse-opts [] scan-options)))))))
