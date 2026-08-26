(ns aeonik.inventory.scanner
  (:require [clojure.string :as str]
            [aeonik.inventory.model :as model])
  (:import (java.io BufferedInputStream IOException)
           (java.nio.file FileVisitOption FileVisitResult Files LinkOption
                          OpenOption Path SimpleFileVisitor StandardOpenOption)
           (java.nio.file.attribute BasicFileAttributes)
           (java.security MessageDigest)
           (java.time Instant)
           (java.util EnumSet)))

(def ^:private no-follow-links
  (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))

(def ^:private hash-open-options
  (into-array OpenOption [StandardOpenOption/READ LinkOption/NOFOLLOW_LINKS]))

(defn- hex-digest [bytes]
  (apply str (map #(format "%02x" (bit-and (int %) 0xff)) bytes)))

(defn hash-path
  "Hash path through Java NIO without invoking one subprocess per file."
  [^Path path algorithm]
  (let [digest (MessageDigest/getInstance algorithm)
        buffer (byte-array (* 1024 1024))]
    (with-open [input (BufferedInputStream.
                       (Files/newInputStream path hash-open-options))]
      (loop []
        (let [read-count (.read input buffer)]
          (when (pos? read-count)
            (.update digest buffer 0 read-count)
            (recur)))))
    (hex-digest (.digest digest))))

(defn- basic-attributes [^Path path]
  (Files/readAttributes path BasicFileAttributes no-follow-links))

(defn- unix-attributes [^Path path]
  (try
    (let [attrs (Files/readAttributes
                 path "unix:dev,ino,mode,nlink" no-follow-links)]
      {:device (get attrs "dev")
       :inode (get attrs "ino")
       :mode (get attrs "mode")
       :hard-link-count (get attrs "nlink")})
    (catch UnsupportedOperationException _ nil)
    (catch IOException _ nil)))

(defn- filesystem-token [^Path path]
  (or (:device (unix-attributes path))
      (try
        (let [store (Files/getFileStore path)]
          [(.name store) (.type store)])
        (catch IOException _ nil))))

(defn- entry-type [^BasicFileAttributes attrs]
  (cond
    (.isRegularFile attrs) "regular_file"
    (.isDirectory attrs) "directory"
    (.isSymbolicLink attrs) "symlink"
    :else "other"))

(defn- relative-path [^Path root ^Path path]
  (str (.relativize root path)))

(defn- parent-path [relative]
  (when-not (str/blank? relative)
    (let [path (.getParent (Path/of relative (make-array String 0)))]
      (if path (str path) ""))))

(defn- base-observation
  [scan-id root-id ^Path root ^Path path ^BasicFileAttributes attrs]
  (let [relative (relative-path root path)
        unix (unix-attributes path)]
    (model/compact
     {:xt/id (model/observation-id scan-id relative)
      :scan_id scan-id
      :root_id root-id
      :relative_path relative
      :relative_path_bytes (model/path-bytes relative)
      :parent_path (parent-path relative)
      :entry_type (entry-type attrs)
      :size (.size attrs)
      :modified_at (.toInstant (.lastModifiedTime attrs))
      :file_key (some-> (.fileKey attrs) str)
      :device (:device unix)
      :inode (:inode unix)
      :mode (:mode unix)
      :hard_link_count (:hard-link-count unix)
      :observed_at (Instant/now)
      :status "observed"})))

(defn- stable-file? [^BasicFileAttributes before ^BasicFileAttributes after]
  (and (= (.size before) (.size after))
       (= (.lastModifiedTime before) (.lastModifiedTime after))
       (= (.fileKey before) (.fileKey after))
       (.isRegularFile after)))

(defn- file-observation
  [scan-id root-id ^Path root ^Path path ^BasicFileAttributes before algorithm]
  (let [base (base-observation scan-id root-id root path before)]
    (try
      (let [digest (hash-path path algorithm)
            after (basic-attributes path)
            stable? (stable-file? before after)]
        (assoc base
               :hash_algorithm algorithm
               :hash_digest digest
               :content_id (model/content-id algorithm digest (.size before))
               :status (if stable? "stable" "unstable")))
      (catch Exception exception
        (assoc base
               :status "error"
               :error_phase "hash"
               :error_class (.getName (class exception))
               :error_message (.getMessage exception))))))

(defn- symlink-observation
  [scan-id root-id ^Path root ^Path path ^BasicFileAttributes attrs]
  (let [base (base-observation scan-id root-id root path attrs)]
    (try
      (assoc base :symlink_target (str (Files/readSymbolicLink path)))
      (catch Exception exception
        (assoc base
               :status "error"
               :error_phase "read_symlink"
               :error_class (.getName (class exception))
               :error_message (.getMessage exception))))))

(defn- failed-observation
  [scan-id root-id ^Path root ^Path path phase exception]
  (let [relative (try
                   (relative-path root path)
                   (catch Exception _ (str path)))]
    (model/compact
     {:xt/id (model/error-observation-id scan-id relative phase)
      :scan_id scan-id
      :root_id root-id
      :relative_path relative
      :relative_path_bytes (model/path-bytes relative)
      :parent_path (parent-path relative)
      :entry_type "unknown"
      :observed_at (Instant/now)
      :status "error"
      :error_phase phase
      :error_class (.getName (class exception))
      :error_message (.getMessage exception)})))

(defn- count-observation [counts observation]
  (-> counts
      (update :observations inc)
      (update (keyword (str "type-" (:entry_type observation))) (fnil inc 0))
      (update (keyword (str "status-" (:status observation))) (fnil inc 0))))

(defn walk!
  "Walk an explicit root and deliver bounded vectors of observations.

  Symbolic links are never followed. Nested filesystems are traversed unless
  :same-filesystem? is true. Returns counts only after the final batch has
  been accepted by consume-batch!."
  [{:keys [root scan-id root-id algorithm batch-size same-filesystem?]
    :or {algorithm "SHA-256" batch-size 1000 same-filesystem? false}}
   consume-batch!]
  (let [^Path root (.normalize (.toAbsolutePath (Path/of (str root)
                                                         (make-array String 0))))
        _ (when-not (Files/exists root no-follow-links)
            (throw (ex-info "Scan root does not exist" {:root (str root)})))
        root-filesystem (filesystem-token root)
        pending (atom [])
        counts (atom {:observations 0})]
    (letfn [(flush! []
              (when (seq @pending)
                (let [batch @pending]
                  (consume-batch! batch)
                  (reset! pending []))))
            (emit! [observation]
              (swap! counts count-observation observation)
              (swap! pending conj observation)
              (when (>= (count @pending) batch-size)
                (flush!)))
            (ordinary-observation [path attrs]
              (case (entry-type attrs)
                "regular_file" (file-observation
                                scan-id root-id root path attrs algorithm)
                "symlink" (symlink-observation
                           scan-id root-id root path attrs)
                (base-observation scan-id root-id root path attrs)))]
      (Files/walkFileTree
       root
       (EnumSet/noneOf FileVisitOption)
       Integer/MAX_VALUE
       (proxy [SimpleFileVisitor] []
         (preVisitDirectory [path attrs]
           (let [at-boundary? (and same-filesystem?
                                   (not= path root)
                                   (not= root-filesystem
                                         (filesystem-token path)))
                 observation (ordinary-observation path attrs)]
             (emit! (if at-boundary?
                      (assoc observation :status "mount_boundary")
                      observation))
             (if at-boundary?
               FileVisitResult/SKIP_SUBTREE
               FileVisitResult/CONTINUE)))

         (visitFile [path attrs]
           (emit! (ordinary-observation path attrs))
           FileVisitResult/CONTINUE)

         (visitFileFailed [path exception]
           (emit! (failed-observation
                   scan-id root-id root path "walk" exception))
           FileVisitResult/CONTINUE)

         (postVisitDirectory [path exception]
           (when exception
             (emit! (failed-observation
                     scan-id root-id root path "post_visit" exception)))
           FileVisitResult/CONTINUE)))
      (flush!)
      @counts)))
