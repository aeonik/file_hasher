(ns aeonik.inventory.service
  (:require [aeonik.inventory.model :as model]
            [aeonik.inventory.scanner :as scanner]
            [aeonik.inventory.store :as store])
  (:import (java.nio.file Path)))

(defn- absolute-path [root]
  (.normalize
   (.toAbsolutePath
    (Path/of (str root) (make-array String 0)))))

(defn run-scan!
  "Coordinate scan lifecycle so only a fully persisted traversal is complete."
  [inventory-store
   {:keys [root root-id host algorithm batch-size same-filesystem?]
    :or {algorithm "SHA-256" batch-size 1000 same-filesystem? false}}]
  (let [root (absolute-path root)
        host (or host (model/host-name))
        root-id (or root-id (model/root-id host root))
        scan-id (model/new-scan-id)
        started-at (model/now)
        scan-document
        {:xt/id scan-id
         :root_id root-id
         :root_path (str root)
         :root_path_bytes (model/path-bytes (str root))
         :host host
         :scanner_version model/scanner-version
         :hash_algorithm algorithm
         :same_filesystem same-filesystem?
         :batch_size batch-size
         :started_at started-at
         :status "running"}]
    (store/start-scan! inventory-store scan-document)
    (try
      (let [counts
            (scanner/walk!
             {:root root
              :scan-id scan-id
              :root-id root-id
              :algorithm algorithm
              :batch-size batch-size
              :same-filesystem? same-filesystem?}
             #(store/write-observations!
               inventory-store scan-id %))
            completion
            (merge {:finished_at (model/now)}
                   counts)]
        (store/complete-scan! inventory-store scan-id completion)
        (assoc completion
               :scan_id scan-id
               :root_id root-id
               :status "complete"))
      (catch Throwable throwable
        (let [failure
              (model/compact
               {:finished_at (model/now)
                :error_class (.getName (class throwable))
                :error_message (.getMessage throwable)})]
          (try
            (store/fail-scan! inventory-store scan-id failure)
            (catch Throwable store-failure
              (.addSuppressed throwable store-failure)))
          (throw throwable))))))
