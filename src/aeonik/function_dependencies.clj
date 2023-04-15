(ns aeonik.function-dependencies
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.reader.edn :as edn]
            [clojure.tools.reader :as reader]
            [clojure.tools.analyzer.jvm :as jvm]
            [clojure.pprint :as pp]
            [clojure.walk :refer [postwalk]]))

(defn extract-fn-calls [form]
  (let [fn-calls (atom #{})]
    (postwalk
     (fn [x]
       (if (and (seq? x)
                (symbol? (first x))
                (str/starts-with? (namespace (first x)) (str *ns*)))
         (do
           (swap! fn-calls conj (first x))
           x)
         x))
     form)
    @fn-calls))

(defn analyze-fn-deps [namespace-sym file-path]
  (let [file-contents (slurp file-path)
        forms (edn/read-string file-contents)]
    (->> forms
         (filter (fn [f] (and (seq? f) (= (first f) 'defn))))
         (map (fn [f] {:name (second f)
                       :body (nthrest f 3)}))
         (reduce
          (fn [acc {:keys [name body]}]
            (assoc acc name (extract-fn-calls body)))
          {}))))

(defn print-dependencies [deps]
  (doseq [[fn-name dependencies] deps]
    (println fn-name "->")
    (doseq [dep dependencies]
      (println "  ->" dep))))

(def file-path (-> "aeonik/file_hasher.clj" io/resource str))
(def deps (analyze-fn-deps 'aeonik.file-hasher (-> "aeonik/file_hasher.clj" io/resource str)))
(print-dependencies deps)

(pp/pprint file-path)

(defn file-contents [file-path] (let [file-contents (slurp file-path)
                                      forms (edn/read-string file-contents)]
                                  forms))

(pp/pprint (file-contents file-path))

(defn analyzed-file [file-contents]
  (jvm/analyze file-contents))

(pp/pprint (analyzed-file (file-contents file-path)))
