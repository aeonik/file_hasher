(ns aeonik.inventory.model
  (:import (java.net InetAddress)
           (java.nio.charset StandardCharsets)
           (java.nio.file Path)
           (java.time Instant)
           (java.util UUID)))

(def scanner-version "0.2.0")

(defn now []
  (Instant/now))

(defn host-name []
  (.getHostName (InetAddress/getLocalHost)))

(defn path-bytes [path]
  (.getBytes (str path) StandardCharsets/UTF_8))

(defn- stable-id [prefix values]
  (str prefix ":"
       (UUID/nameUUIDFromBytes
        (.getBytes (pr-str values) StandardCharsets/UTF_8))))

(defn new-scan-id []
  (str "scan:" (UUID/randomUUID)))

(defn root-id [host ^Path root]
  (stable-id "root" [host (str (.normalize (.toAbsolutePath root)))]))

(defn observation-id [scan-id relative-path]
  (stable-id "observation" [scan-id relative-path]))

(defn error-observation-id [scan-id relative-path phase]
  (stable-id "observation-error" [scan-id relative-path phase]))

(defn content-id [algorithm digest size]
  (stable-id "content" [algorithm digest size]))

(defn compact
  "Remove nil values before handing a sparse document to XTDB."
  [document]
  (into {} (remove (comp nil? val)) document))
