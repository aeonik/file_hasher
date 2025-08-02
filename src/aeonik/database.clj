(ns aeonik.database
  (:require [clojure.java.jdbc :as jdbc]
            [aeonik.config :as c]
            [aeonik.parsers :as p]))


(def db-spec
  {:subprotocol "postgresql"
   :subname     "//localhost:5432/file_hasher"
   :user        "file_hasher"
   :password    (:db-password c/config)})

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

(comment (defn load-files-into-db [files prefix batch-size]
  (let [data (map #(build-data-row (p/parse-line % prefix)) files)]
    (insert-data-batch data batch-size))))

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

(comment (defn export-duplicate-paths [output-file]
  (println "Exporting duplicate paths to file...")
  (let [results (fetch-duplicate-paths)]
    (save-results-to-file results output-file))))
