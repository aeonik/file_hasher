(ns aeonik.parsers
  (:require [clojure.string :as str])
  (:import (java.util.regex Pattern)
           (org.apache.commons.io FilenameUtils)))

(defn strip-path-prefix [path prefix]
  (str/replace path (re-pattern (str (Pattern/quote prefix) "(.*?)$")) "$1"))

(defn parse-openssl-line [line]
  (let [pattern #"^SHA2-256\((.+)\)= ([a-fA-F0-9]{64})$"
        matcher (re-matcher pattern line)]
    (when-let [[_ path hash] (re-find matcher)]
      [hash path])))

(defn parse-sha256sum-line [line]
  (let [[hash path] (str/split line #"\s+" 2)]
    [hash path]))

(defn parse-line
  ([hash path] (parse-line hash path nil))
  ([hash path prefix]
   (let [basename  (FilenameUtils/getBaseName path)
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

(defn process-line [line prefix format-fn]
  (let [[hash path] (format-fn line)]
    (parse-line hash path prefix)))

(comment
  ;; Example of parsing an OpenSSL output line using the parse-openssl-line function.
  ;; This line contains the path within parentheses and is prefixed by "SHA2-256", followed by the hash.
  (let [openssl-line "SHA2-256(/run/media/dave/backup_nvme/archives/ArchiveSeagateBig/Music/the black keys/[2006] chulahoma ~ the songs of junior kimbrough [EP]/[2006]-chulahoma-lg.jpg)= f7f53bb8703c59586e1b8d589543f1d221aae2abfaed9d575630df4109d06d42"
        [hash path] (parse-openssl-line openssl-line)]
    ;; Parse the line to extract the hash and path
    (parse-line hash path))
  ;; The output will be a map with the path, dirname, basename, extension, and hash.
  )

(comment
  ;; Example of parsing a sha256sum output line using the parse-sha256sum-line function.
  ;; This line starts directly with the hash followed by spaces and then the file path.
  (let [sha256sum-line "f7f53bb8703c59586e1b8d589543f1d221aae2abfaed9d575630df4109d06d42  /run/media/dave/backup_nvme/archives/ArchiveSeagateBig/Music/the black keys/[2006] chulahoma ~ the songs of junior kimbrough [EP]/[2006]-chulahoma-lg.jpg"
        [hash path] (parse-sha256sum-line sha256sum-line)]
    ;; Parse the line to extract the hash and path
    (parse-line hash path))
  ;; The output will be a map with the path, dirname, basename, extension, and hash.
  )

(comment
  ;; Assuming you have a line and you've determined it's an OpenSSL formatted line.
  (let [openssl-line "SHA2-256(/run/media/dave/backup_nvme/archives/ArchiveSeagateBig/Music/the black keys/[2006] chulahoma ~ the songs of junior kimbrough [EP]/[2006]-chulahoma-lg.jpg)= f7f53bb8703c59586e1b8d589543f1d221aae2abfaed9d575630df4109d06d42"
        ;; Call process-line with the openssl-line and the parse-openssl-line function.
        result (process-line openssl-line nil parse-openssl-line)]
    ;; Here's what you get back
    result)
  ;; The result is a map constructed by parse-line after extracting the hash and path using parse-openssl-line.

  ;; Similarly, for a sha256sum formatted line.
  (let [sha256sum-line "f7f53bb8703c59586e1b8d589543f1d221aae2abfaed9d575630df4109d06d42  /run/media/dave/backup_nvme/archives/ArchiveSeagateBig/Music/the black keys/[2006] chulahoma ~ the songs of junior kimbrough [EP]/[2006]-chulahoma-lg.jpg"
        result (process-line sha256sum-line nil parse-sha256sum-line)]
    result)
  ;; Again, the result is a map constructed by parse-line, this time using parse-sha256sum-line for extraction.
  )
