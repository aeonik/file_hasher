(ns aeonik.config
  (:require [clojure.tools.reader.edn :as edn]))

(def config (edn/read-string (slurp "resources/secrets.edn")))
