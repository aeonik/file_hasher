(ns aeonik.inventory.xtdb
  (:require [xtdb.api :as xt]
            [aeonik.inventory.store :as store]))

(defn- put-docs-operation [table documents]
  (when (seq documents)
    (into [:put-docs table] documents)))

(defrecord XtdbStore [connectable]
  store/InventoryStore
  (start-scan! [_ scan-document]
    (xt/execute-tx
     connectable
     [[:put-docs :inventory_scans scan-document]]
     {:metadata {:component "file_hasher"
                 :operation "start_scan"
                 :scan_id (:xt/id scan-document)}}))

  (write-observations! [_ scan-id observations]
    (when-let [operation
               (put-docs-operation :inventory_observations observations)]
      (xt/execute-tx
       connectable
       [operation]
       {:metadata {:component "file_hasher"
                   :operation "write_observations"
                   :scan_id scan-id
                   :observation_count (count observations)}})))

  (complete-scan! [_ scan-id completion]
    (xt/execute-tx
     connectable
     [[:patch-docs :inventory_scans
       (assoc completion :xt/id scan-id :status "complete")]]
     {:metadata {:component "file_hasher"
                 :operation "complete_scan"
                 :scan_id scan-id}}))

  (fail-scan! [_ scan-id failure]
    (xt/execute-tx
     connectable
     [[:patch-docs :inventory_scans
       (assoc failure :xt/id scan-id :status "failed")]]
     {:metadata {:component "file_hasher"
                 :operation "fail_scan"
                 :scan_id scan-id}})))

(defn store [connectable]
  (->XtdbStore connectable))

(defn client
  [{:keys [host port database user password]
    :or {host "127.0.0.1" port 5432 database "xtdb"}}]
  (xt/client
   (cond-> {:host host
            :port port
            :dbname database}
     user (assoc :user user)
     password (assoc :password password))))

(defn status [connectable]
  (xt/status connectable))

(defn scan-by-id [connectable scan-id]
  (first
   (xt/q connectable
         ["SELECT * FROM inventory_scans WHERE _id = ?" scan-id])))

(defn observations-for-scan [connectable scan-id]
  (xt/q connectable
        ["SELECT * FROM inventory_observations WHERE scan_id = ?" scan-id]))
